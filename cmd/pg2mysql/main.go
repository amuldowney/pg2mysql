package main

import (
	"log"

	"github.com/amuldowney/pg2mysql/commands"
	flags "github.com/jessevdk/go-flags"
)

func main() {
	parser := flags.NewParser(&commands.PG2MySQL, flags.HelpFlag)
	parser.NamespaceDelimiter = "-"

	_, err := parser.Parse()
	if err != nil {
		log.Fatalf("error: %s", err)
	}
}
