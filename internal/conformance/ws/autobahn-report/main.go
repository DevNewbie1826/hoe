package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/DevNewbie1826/hoe/internal/conformance/ws/report"
)

func main() {
	dir := flag.String("reports", "", "Autobahn report directory")
	expected := flag.String("expected", "", "exact expected case IDs JSON")
	summary := flag.String("summary", "autobahn-summary.txt", "summary output path")
	prepare := flag.String("prepare", "", "partition this Autobahn specification")
	batches := flag.String("batches", "", "batch specification directory")
	merge := flag.String("merge", "", "merge this batch directory")
	flag.Parse()
	if *prepare != "" || *merge != "" {
		var err error
		if *prepare != "" {
			err = report.Prepare(*prepare, *expected, *batches)
		} else {
			err = report.Merge(*merge, *dir)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	text, err := report.Summarize(*dir, *expected)
	if text != "" {
		fmt.Print(text)
		if writeErr := os.WriteFile(*summary, []byte(text), 0600); writeErr != nil {
			fmt.Fprintln(os.Stderr, writeErr)
			os.Exit(1)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
