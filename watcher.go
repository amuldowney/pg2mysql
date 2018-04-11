package pg2mysql

import (
	"fmt"
	"strings"
)

//go:generate counterfeiter . VerifierWatcher

type VerifierWatcher interface {
	TableVerificationDidStart(tableName string)
	TableVerificationDidFinish(tableName string, missingRows int64, missingIDs []string)
	TableVerificationDidFinishWithError(tableName string, err error)
}

//go:generate counterfeiter . MigratorWatcher

type MigratorWatcher interface {
	WillBuildSchema()
	DidBuildSchema()

	WillDisableConstraints()
	DidDisableConstraints()

	WillEnableConstraints()
	EnableConstraintsDidFinish()
	EnableConstraintsDidFailWithError(err error)

	WillTruncateTable(tableName string)
	TruncateTableDidFinish(tableName string)

	TableMigrationDidStart(tableName string)
	TableMigrationDidFinish(tableName string, recordsInserted int64)

	TableMigrationWithID(tableName string, identifier string)
	TableMigrationWithoutID(tableName string)

	DidMigrateRow(tableName string)
	DidFailToMigrateRowWithError(tableName string, err error)

	PrintStatement(stmt string)
}

func NewStdoutPrinter() *StdoutPrinter {
	return &StdoutPrinter{}
}

type StdoutPrinter struct{}

func (s *StdoutPrinter) TableVerificationDidStart(tableName string) {
	fmt.Printf("Verifying table %s...", tableName)
}

func (s *StdoutPrinter) TableVerificationDidFinish(tableName string, missingRows int64, missingIDs []string) {
	if missingRows != 0 {
		if missingRows == 1 {
			fmt.Println("\n\tFAILED: 1 row missing")
		} else {
			fmt.Printf("\n\tFAILED: %d rows missing\n", missingRows)
		}
		if missingIDs != nil {
			fmt.Printf("\tMissing IDs: %v\n", strings.Join(missingIDs, ","))
		}
	} else {
		s.done()
	}
}

func (s *StdoutPrinter) done() {
	fmt.Println("OK")
}

func (s *StdoutPrinter) TableVerificationDidFinishWithError(tableName string, err error) {
	fmt.Printf("failed: %s", err)
}

func (s *StdoutPrinter) WillBuildSchema() {
	fmt.Print("Building schema...")
}

func (s *StdoutPrinter) DidBuildSchema() {
	s.done()
}

func (s *StdoutPrinter) WillDisableConstraints() {
	fmt.Print("Disabling constraints...")
}

func (s *StdoutPrinter) DidDisableConstraints() {
	s.done()
}

func (s *StdoutPrinter) DidFailToDisableConstraints(err error) {
	s.done()
}

func (s *StdoutPrinter) WillEnableConstraints() {
	fmt.Print("Enabling constraints...")
}

func (s *StdoutPrinter) EnableConstraintsDidFailWithError(err error) {
	fmt.Printf("failed: %s", err)
}

func (s *StdoutPrinter) EnableConstraintsDidFinish() {
	s.done()
}

func (s *StdoutPrinter) WillTruncateTable(tableName string) {
	fmt.Printf("Truncating %s...", tableName)
}

func (s *StdoutPrinter) TruncateTableDidFinish(tableName string) {
	s.done()
}

func (s *StdoutPrinter) TableMigrationDidStart(tableName string) {
	fmt.Printf("Migrating %s...", tableName)
}

func (s *StdoutPrinter) TableMigrationWithID(tableName string, identifier string) {
	fmt.Printf("\nFound identifier:%s to help migrate table:%s\n", identifier, tableName)
}

func (s *StdoutPrinter) TableMigrationWithoutID(tableName string) {
	fmt.Printf("\nNo identifying column found, manual merge for table %s\n", tableName)
}

func (s *StdoutPrinter) PrintStatement(stmt string) {
	fmt.Printf("\nProcessing Statement:%s\n", stmt)
}

func (s *StdoutPrinter) TableMigrationDidFinish(tableName string, recordsInserted int64) {
	switch recordsInserted {
	case 0:
		fmt.Println("OK (0 records inserted)")
	case 1:
		fmt.Println("OK\n  inserted 1 row")
	default:
		fmt.Printf("OK\n  inserted %d rows\n", recordsInserted)
	}
}

func (s *StdoutPrinter) DidMigrateRow(tableName string) {
	fmt.Printf(".")
}

func (s *StdoutPrinter) DidFailToMigrateRowWithError(tableName string, err error) {
	fmt.Printf("x")
}
