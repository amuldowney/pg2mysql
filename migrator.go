package pg2mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
)

type Migrator interface {
	Migrate() error
}

func NewMigrator(src, dst DB, truncateFirst bool, watcher MigratorWatcher) Migrator {
	return &migrator{
		src:           src,
		dst:           dst,
		truncateFirst: truncateFirst,
		watcher:       watcher,
	}
}

type migrator struct {
	src, dst      DB
	truncateFirst bool
	watcher       MigratorWatcher
}

func (m *migrator) Migrate() error {
	srcSchema, err := BuildSchema(m.src)
	if err != nil {
		return fmt.Errorf("failed to build source schema: %s", err)
	}

	m.watcher.WillDisableConstraints()
	err = m.dst.DisableConstraints()
	if err != nil {
		return fmt.Errorf("failed to disable constraints: %s", err)
	}
	m.watcher.DidDisableConstraints()

	defer func() {
		m.watcher.WillEnableConstraints()
		err = m.dst.EnableConstraints()
		if err != nil {
			m.watcher.EnableConstraintsDidFailWithError(err)
		} else {
			m.watcher.EnableConstraintsDidFinish()
		}
	}()

	for _, table := range srcSchema.Tables {
		if m.truncateFirst {
			m.watcher.WillTruncateTable(table.Name)
			_, err := m.dst.DB().Exec(fmt.Sprintf("TRUNCATE TABLE %s", table.Name))
			if err != nil {
				return fmt.Errorf("failed truncating: %s", err)
			}
			m.watcher.TruncateTableDidFinish(table.Name)
		}

		columnNamesForInsert := make([]string, len(table.Columns))
		placeholders := make([]string, len(table.Columns))
		for i := range table.Columns {
			columnNamesForInsert[i] = fmt.Sprintf("`%s`", table.Columns[i].Name)
			placeholders[i] = "?"
		}
		preparedInsert := fmt.Sprintf("(%s)", strings.Join(placeholders, ","))
		preparedInserts := make([]string, 100)
		for i, _ := range preparedInserts {
			preparedInserts[i] = preparedInsert
		}
		preparedStmtMax, err := m.dst.DB().Prepare(fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES %s",
			table.Name,
			strings.Join(columnNamesForInsert, ","),
			strings.Join(preparedInserts, ","),
		))

		preparedStmt, err := m.dst.DB().Prepare(fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			table.Name,
			strings.Join(columnNamesForInsert, ","),
			strings.Join(placeholders, ","),
		))

		if err != nil {
			return fmt.Errorf("failed creating prepared statement: %s", err)
		}

		var recordsInserted int64

		m.watcher.TableMigrationDidStart(table.Name)

		if table.HasColumn("id") {
			m.watcher.TableMigrationWithID(table.Name, "id")
			err = migrateWithIDs(m.watcher, m.src, m.dst, table, "id", &recordsInserted, preparedStmt, preparedStmtMax)
			if err != nil {
				return fmt.Errorf("failed migrating table with ids: %s", err)
			}
		} else if table.HasColumn("uuid") {
			m.watcher.TableMigrationWithID(table.Name, "uuid")
			err = migrateWithIDs(m.watcher, m.src, m.dst, table, "uuid", &recordsInserted, preparedStmt, preparedStmtMax)
			if err != nil {
				return fmt.Errorf("failed migrating table with ids: %s", err)
			}
		} else {
			m.watcher.TableMigrationWithoutID(table.Name)
			err = EachMissingRow(m.src, m.dst, table, func(scanArgs []interface{}) {
				count, err := insert(preparedStmt, scanArgs)
				if err != nil {
					fmt.Fprintf(os.Stderr, "failed to insert into %s: %s\n", table.Name, err)
					return
				}
				recordsInserted += count
			})
			if err != nil {
				return fmt.Errorf("failed migrating table without (uu)ids: %s", err)
			}
		}

		m.watcher.TableMigrationDidFinish(table.Name, recordsInserted)
	}

	return nil
}

func migrateWithIDs(
	watcher MigratorWatcher,
	src DB,
	dst DB,
	table *Table,
	identifier string,
	recordsInserted *int64,
	preparedStmt *sql.Stmt,
	preparedStmtMax *sql.Stmt,
) error {
	columnNamesForSelect := make([]string, len(table.Columns))
	//values := make([]interface{}, len(table.Columns))
	//scanArgs := make([]interface{}, len(table.Columns))
	for i := range table.Columns {
		columnNamesForSelect[i] = table.Columns[i].Name
		//scanArgs[i] = &values[i]
	}

	// find ids already in dst
	rows, err := dst.DB().Query(fmt.Sprintf("SELECT %s FROM %s", identifier, table.Name))
	if err != nil {
		return fmt.Errorf("failed to select %s from rows: %s", identifier, err)
	}

	var dstIDs []string
	for rows.Next() {
		var id *string
		if err = rows.Scan(&id); err != nil {
			return fmt.Errorf("failed to scan %s from row: %s", identifier, err)
		}
		dstIDs = append(dstIDs, (fmt.Sprintf("'%s'", *id)))
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed iterating through rows: %s", err)
	}

	if err = rows.Close(); err != nil {
		return fmt.Errorf("failed closing rows: %s", err)
	}

	// select data for ids to migrate from src
	stmt := fmt.Sprintf(
		"SELECT %s FROM %s",
		strings.Join(columnNamesForSelect, ","),
		table.Name,
	)

	if len(dstIDs) > 0 {
		stmt = fmt.Sprintf("%s WHERE %s NOT IN (%s)", stmt, identifier, strings.Join(dstIDs, ","))
	}
	watcher.PrintStatement(stmt)
	rows, err = src.DB().Query(stmt)
	if err != nil {
		return fmt.Errorf("failed to select rows: %s", err)
	}

	argsArray := make([]interface{}, len(table.Columns)*100)
	argsIndex := 0
	for rows.Next() {
		values := make([]interface{}, len(table.Columns))
		scanArgs := make([]interface{}, len(table.Columns))
		for i := range table.Columns {
			//columnNamesForSelect[i] = table.Columns[i].Name
			scanArgs[i] = &values[i]
		}

		if err = rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("failed to scan row: %s", err)
		}

		argsArray = append(argsArray, scanArgs...)
		argsIndex++

		if argsIndex >= 99 {
			numInserted, err := insert(preparedStmtMax, scanArgs)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to insert into %s: %s\n", table.Name, err)
				continue
			}
			*recordsInserted += numInserted
			argsArray = make([]interface{}, len(table.Columns)*100)
			argsIndex = 0
		}
	}
	//we have some leftovers
	//and they're a total mess inside argsArray, len(columns worth etc)
	for i := 0; i < len(argsArray); i += len(table.Columns) {
		numInserted, err := insert(preparedStmt, argsArray[i:i+len(table.Columns)])
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to insert into %s: %s\n", table.Name, err)
			continue
		}
		*recordsInserted += numInserted
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("failed iterating through rows: %s", err)
	}

	if err = rows.Close(); err != nil {
		return fmt.Errorf("failed closing rows: %s", err)
	}

	return nil
}

func insert(stmt *sql.Stmt, values []interface{}) (int64, error) {

	result, err := stmt.Exec(values...)
	if err != nil {
		return 0, fmt.Errorf("failed to exec stmt: %s", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed getting rows affected by insert: %s", err)
	}

	if rowsAffected == 0 {
		return 0, errors.New("no rows affected by insert")
	}

	return rowsAffected, nil
}
