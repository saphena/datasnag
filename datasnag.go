package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	yaml "gopkg.in/yaml.v2"
)

var Inpfile = flag.String("in", "\\temp\\overnightoffsite.sql", "The input SQL file")
var Outfile = flag.String("out", "sqlite.sql", "The output SQL file")
var bk = flag.Int("bk", 3000, "Size of buffer in 1024 byte chunks")

var yml = `
DropTables: [archiveddocs,deleteddocs]

`
var cfg struct {
	DropTables []string `yaml:"DropTables"`
}

const appversion = "Datasnag v0.1"

var mybuffersize = 4000 * 1024
var W *bufio.Writer
var CommitNeeded = false

func main() {

	fmt.Printf("%v Copyright (c) 2022 Bob Stammers\n", appversion)
	flag.Parse()

	mybuffersize = *bk * 1024

	D := yaml.NewDecoder(strings.NewReader(yml))
	D.Decode(&cfg)
	fmt.Printf("%v\n", cfg)

	processFile(*Inpfile)

}

func processFile(f string) {

	Ifile, err := os.Open(f)

	if err != nil {
		log.Fatalf("failed opening file: %s", err)
	}
	defer Ifile.Close()

	ofile, err := os.Create(*Outfile)

	if err != nil {
		log.Fatalf("failed opening file: %s", err)
	}
	defer ofile.Close()

	W = bufio.NewWriter(ofile)
	defer W.Flush()

	scanner := bufio.NewScanner(Ifile)
	scanner.Split(bufio.ScanLines)
	buf := make([]byte, mybuffersize)
	scanner.Buffer(buf, mybuffersize)
	var x string
	for scanner.Scan() {
		x = scanner.Text()
		xx := strings.Split(x, " ")
		if len(xx) < 1 {
			continue
		}
		lx := len(xx)
		tnameix := 0
		switch xx[0] {
		case "DROP":
			tnameix = 2
			if lx > 2 {
				if xx[2] == "IF" {
					tnameix = 4
				}
			}
		case "CREATE":
			tnameix = 2
		case "INSERT":
			tnameix = 2
			if lx > 2 {
				if xx[2] == "INTO" {
					tnameix = 3
				}
			}
		case "LOCK":
			tnameix = 2
		}
		processLine := true
		tname := ""
		if tnameix > 0 && tnameix < lx {
			tname = xx[tnameix]
			for _, dt := range cfg.DropTables {
				if tname == "`"+dt+"`" {
					processLine = false
				}
			}
		}
		if !processLine {
			//fmt.Printf("Dropping line belonging to %v\n", tname)
			continue
		}
		switch xx[0] {
		case "CREATE":
			processCreateLine(x)
		case "DROP":
			processDropLine(x)
		case "INSERT":
			processInsertLine(x)
		case "LOCK":
			processLockLine(x)
		case "UNLOCK":
			processUnlockLine(x)
		}

	}
	z := len(x)
	if z > 100 {
		z = 100
	}
	y := x[:z]
	fmt.Printf("\n%v\n\nAll done\n\n%v\n", y, scanner.Err())

}

func processCreateLine(x string) {

}

func processDropLine(x string) {

}

func processInsertLine(x string) {

}

func processLockLine(x string) {

	W.WriteString("BEGIN TRANSACTION;\n")
	CommitNeeded = true
}

func processUnlockLine(x string) {

	if CommitNeeded {
		W.WriteString("COMMIT;\n")
	}
	CommitNeeded = false

}
