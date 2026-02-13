package main

import (
	"fmt"
	"os"
)

var version = "dev"

const usage = `sql-tap — Watch SQL traffic in real-time

Usage:
  sql-tap <addr>                    Monitor SQL traffic
  sql-tap version                   Show version
  sql-tap help                      Show this help
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}

	switch os.Args[1] {
	case "version", "--version", "-v":
		fmt.Printf("sql-tap %s\n", version)
		return
	case "help", "--help", "-h":
		fmt.Fprint(os.Stderr, usage)
		return
	}

	monitor(os.Args[1], os.Args[2:])
}

func monitor(addr string, args []string) {
	fmt.Fprintf(os.Stdout, "not implemented yet\n")
}
