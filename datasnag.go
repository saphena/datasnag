package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	yaml "gopkg.in/yaml.v2"
)

var Inpfile = flag.String("in", "\\temp\\overnightoffsite.sql", "The input SQL file")
var Outfile = flag.String("out", "sqlite.sql", "The output SQL file")
var bk = flag.Int("bk", 4000, "Size of buffer in 1024 byte chunks")
var maxInserts = flag.Int("max", -1, "Max number of INSERTs to process")
var onlyTable = flag.String("table", "", "Only process this table")
var useTrans = flag.Int("trans", 0, "0=no transactions' 1=single transaction; -1=per table transactions")
var verbose = flag.Bool("v", false, "Show more messages")
var imgFolder = flag.String("images", "\\temp\\crap", "Folder to write image files")
var imgNumber = flag.Int("imgnum", 1, "First number to use for image files")
var cfgFile = flag.String("cfg", "", "File containing YAML configuration")

var yml = `
DropTables: [archiveddocs,	# Don't care
			deleteddocs,
			"switchboard items",
			tblletters,		# superseded
			tletterqq,		# transactional only, bad export
			tletterq,		# transaction, defunct, bad export. Still referenced but not actually used as such
			tmessages,		# transactional, formatting
			callblockers,	# defunct
			cardchecks,
			cc_notify,		# defunct?
			cleansebatches,
			cleansedata,
			dd_capture, dd_capture_history,
			dd_history,
			dd_notify,		# transactional
			docarchives,
			fplandata,		# defunct
			fyearplans,		# defunct
			qryfetchactivedds2live,
			rejected_data_records,
			tblddtransactions,	# transactional
			tblduplicates,		# defunct
			tblplankeys,			# looks uninteresting
			tblsources,			# data transactional
			paymentplans,		# superseded by tcontracts
			tmp_itemtable, tmp_live, tmptab,
			tnewplanq,
			tplancallouts,
			tprospectlog,		# defunct
			tsalesagents,		# defunct
			xlsimport,
			]

# These words are not wanted in CREATE statements
DropWords: 	[unsigned,COMMENT,CHARSET,ENGINE,DEFAULT,AUTO_INCREMENT,NOT,"NULL",latin1,utf8,PRIMARY,UNIQUE,",",ROW_FORMAT,DYNAMIC,CURRENT_TIMESTAMP]

# These words involve skipping the next token as well
SkipWords: [COMMENT,CHARSET,ENGINE,DEFAULT,AUTO_INCREMENT]

SkipClauses: [PRIMARY,UNIQUE,KEY,ON]

ExtractImageTables: [{ "name":"tcustomerdocs", "ix":4, "year":2, "plan": 1 }]

# Image names can be anything but can include Year scanned, Plan number, relative serial number
ImageFilename: "y%v-p%v-i%v.jpg"

FlagOK: true

`

type ImageTable struct {
	Tablename    string `yaml:"name"`
	ImageFieldIx int    `yaml:"ix"`
	YearFieldIx  int    `yaml:"year"`
	PlanFieldIx  int    `yaml:"plan"`
}

var cfg struct {
	DropTables    []string     `yaml:"DropTables"`
	DropWords     []string     `yaml:"DropWords"`
	SkipWords     []string     `yaml:"SkipWords"`
	SkipClauses   []string     `yaml:"SkipClauses"`
	ImageTables   []ImageTable `yaml:"ExtractImageTables"`
	ImageFilename string       `yaml:"ImageFilename"`
	OK            bool         `yaml:"FlagOK"`
}

const appversion = "Datasnag v0.1"

var mybuffersize = 4000 * 1024
var W *bufio.Writer
var CommitNeeded = false
var CreateInProgress = false
var SkipNextToken = false
var CommaWaiting = false
var ImagesStored = 0

func fileExists(x string) bool {

	_, err := os.Stat(x)
	return err == nil

}

func loadConfig() bool {

	var D *yaml.Decoder

	if *cfgFile != "" && !fileExists(*cfgFile) {
		fmt.Printf("Can't find config file %v\n", *cfgFile)
		return false
	}

	if *cfgFile != "" {
		file, err := os.Open(*cfgFile)
		if err != nil {
			return false
		}
		defer file.Close()
		D = yaml.NewDecoder(file)
	} else {
		D = yaml.NewDecoder(strings.NewReader(yml))
	}

	D.Decode(&cfg)
	if *verbose || !cfg.OK {
		fmt.Printf("%v\n", cfg)
	}
	if !cfg.OK {
		fmt.Println("Configuration not good, please fix and try again")
		return false
	}

	return true

}
func main() {

	fmt.Printf("%v Copyright (c) 2022 Bob Stammers\n", appversion)
	flag.Parse()

	if !loadConfig() {
		return
	}

	mybuffersize = *bk * 1024

	processFile(*Inpfile)
	if ImagesStored > 0 {
		fmt.Printf("%v images stored in %v\n", ImagesStored, *imgFolder)
		fmt.Printf("Next time use -imgnum %v\n", *imgNumber)
	}

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
	numInserts := 0
	if *useTrans == 1 {
		W.WriteString("BEGIN TRANSACTION\n")
	}
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
			if *onlyTable != "" && tname != "`"+*onlyTable+"`" {
				processLine = false
			} else {
				for _, dt := range cfg.DropTables {
					if tname == "`"+dt+"`" {
						processLine = false
					}
				}
			}
		}
		if !processLine {
			if *verbose && xx[0] == "CREATE" {
				fmt.Printf("Dropping %v\n", tname)
			}
			continue
		}
		isImg := false
		imgCol := 0
		yearCol := 0
		planCol := 0
		for _, i := range cfg.ImageTables {
			if tname == "`"+i.Tablename+"`" {
				isImg = true
				imgCol = i.ImageFieldIx
				yearCol = i.YearFieldIx
				planCol = i.PlanFieldIx
				break
			}
		}
		switch xx[0] {
		case "CREATE":
			if *verbose {
				fmt.Printf("Creating %v\n", tname)
			}
			CreateInProgress = true
			processCreateLine(x)
		case "DROP":
			processDropLine(x)
		case "INSERT":
			processInsertLine(x, isImg, imgCol, yearCol, planCol)
			numInserts++
			if *maxInserts > 0 && numInserts > *maxInserts {
				return
			}
		case "LOCK":
			processLockLine(x)
		case "UNLOCK":
			processUnlockLine(x)
		default:
			if CreateInProgress {
				processCreateLine(x)
			}
		}

	}
	if *useTrans == 1 {
		W.WriteString("COMMIT;\n")
	}
	z := len(x)
	if z > 100 {
		z = 100
	}
	y := x[:z]
	fmt.Printf("\n%v\n\nAll done\n\n%v\n", y, scanner.Err())

}

func processCreateLine(x string) {

	re := regexp.MustCompile(`(\w+|` + "`" + `[\w\s]+` + "`" + `|'.*'|\(|\)|,)`)
	rel := regexp.MustCompile(`;$`)
	CreateInProgress = !rel.MatchString(x)
	rex := re.FindAllString(x, -1)

	//fmt.Println()

	for _, rx := range rex {

		//fmt.Printf("{%v} ", rx)
		if rx == "," {
			SkipNextToken = false
		}
		if SkipNextToken {
			SkipNextToken = false
			continue
		}
		skip := false
		for _, s := range cfg.SkipClauses {
			if s == rx {
				skip = true
			}
		}
		if skip {
			CommaWaiting = false
			break
		}

		if CommaWaiting {
			CommaWaiting = false
			W.WriteString(",")
		}

		for _, s := range cfg.SkipWords {
			if s == rx {
				SkipNextToken = true
			}
		}
		drop := false
		for _, s := range cfg.DropWords {
			if s == rx {
				drop = true
			}
		}
		if rx == "," {
			CommaWaiting = true
		}
		if !drop {
			//fmt.Printf("[.%v.] ", rx)
			W.WriteString(rx + " ")
		}
	}
	if !CreateInProgress {
		W.WriteString(";\n")
	}

}

func processDropLine(x string) {

	W.WriteString("\n" + x)
}

func processInsertLine(x string, img bool, ix int, yearix int, planix int) {

	if !img {
		y := strings.ReplaceAll(x, `\'`, "''")
		W.WriteString(y)
		return
	}

	y := strings.ReplaceAll(x, `\'`, "~~") // Let's just cheat
	re := regexp.MustCompile(`([^\(]+)\(([^\)]+)`)
	rex := re.FindStringSubmatch(y)
	W.WriteString(rex[1] + "(")

	re2 := regexp.MustCompile(`('[^']*'|[^,]*),?`)

	rey := re2.FindAllStringSubmatch(rex[2], -1)
	started := false
	year := "0000"
	plan := "0"
	for rx, ry := range rey {
		if started {
			W.WriteString(",")
		}
		if rx != ix {
			if rx == yearix {
				year = ry[1][1:5]
			}
			if rx == planix {
				plan = ry[1]
			}
			//fmt.Printf("rey %v: %v\n", rx, ry)
			W.WriteString(strings.ReplaceAll(ry[1], `~~`, "''"))
		} else {
			imgname := storeImage([]byte(ry[1][2:]), year, plan)
			W.WriteString("'" + imgname + "'")
		}
		started = true
	}
	W.WriteString(");\n")

}

func processLockLine(x string) {

	if *useTrans < 0 {
		W.WriteString("BEGIN TRANSACTION;\n")
		CommitNeeded = true
	}
}

func processUnlockLine(x string) {

	if CommitNeeded {
		W.WriteString("COMMIT;\n")
	}
	CommitNeeded = false

}

func storeImage(hexbytes []byte, year string, plan string) string {

	fname := fmt.Sprintf(cfg.ImageFilename, year, plan, *imgNumber)
	*imgNumber++
	fpath := filepath.Join(*imgFolder, fname)
	f, err := os.Create(fpath)
	if err != nil {
		fmt.Printf("can't create %v (%v)\n", fpath, err)
		return fname
	}
	defer f.Close()
	buf := make([]byte, hex.DecodedLen(len(hexbytes)))
	_, err = hex.Decode(buf, hexbytes)
	if err != nil {
		fmt.Printf("can't decode hex %v [%v]\n", hexbytes[0:20], err)
		return fname
	}
	f.Write(buf)
	ImagesStored++
	return fname
}
