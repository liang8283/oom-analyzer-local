package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/lib/pq"
	"io"
	"os"
	"path"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const ddlString string = `create table %s
(
logtime        timestamp with time zone,
loguser        text,
logdatabase    text,
logpid         text,
logthread      text,
loghost        text,
logport        text,
logsessiontime text,
logtransaction bigint,
logsession     text,
logcmdcount    text,
logsegment     text,
logslice       text,
logdistxact    text,
loglocalxact   text,
logsubxact     text,
logseverity    text,
logstate       text,
account_name   text,
child_id       bigint,
parent_id      bigint,
quota          bigint,
peak           bigint,
allocated      bigint,
freed          bigint,
current        bigint
);`

const copyStr string = "copy %s.memlog from '%s' delimiter ',';"

var copyCols = []string{"logtime",
	"loguser",
	"logdatabase",
	"logpid",
	"logthread",
	"loghost",
	"logport",
	"logsessiontime",
	"logtransaction",
	"logsession",
	"logcmdcount",
	"logsegment",
	"logslice",
	"logdistxact",
	"loglocalxact",
	"logsubxact",
	"logseverity",
	"logstate",
	"account_name",
	"child_id",
	"parent_id",
	"quota",
	"peak",
	"allocated",
	"freed",
	"current",
}

var dbConn *sql.DB

type SessRow struct {
	SessID     string
	CmdID      string
	PercentMem float64
}

type MemAcctRow struct {
	AccountName string
	PercentMem  float64
}

type SegmentRpt struct {
	SegID     string
	OOMEvents []OOMEventRpt
}

type OOMEventRpt struct {
	Timestamp time.Time
	Sessions  []SessionRpt
}

type SessionRpt struct {
	SessRow
	MemAccounts []MemAcctRow
}

type MemAcctRtp struct {
	AccountName string
	MemUsed     float64
}

func dbExists(dbname string) bool {
	LOG.Debugf("Testing for db\b")
	var datname string
	err := dbConn.QueryRow("select datname from pg_database where datname = $1", dbname).Scan(&datname)
	switch {
	case err == sql.ErrNoRows:
		LOG.Debugf("No DB Named: %s\n", dbname)
		return false
	case err != nil:
		LOG.Fatalf("Error connecting to postgres: %v\n", err)
	default:
		LOG.Debugf("Found DB Named: %s\n", dbname)
		return true
	}
	return false
}

func execSQL(sql string) {
	if _, err := dbConn.Exec(sql); err != nil {
		LOG.Fatalf("Error executing sql: %v\n", err)
	}
}

func connectToDB(dbname string) {
	var err error
	// dbConn, err = sql.Open("postgres", "sslmode=disable dbname="+dbname)
	dbConn, err = sql.Open("postgres", "user=gpadmin password=changeme host=10.152.9.2 port=2019 sslmode=disable dbname="+dbname)
	if err != nil {
		LOG.Fatalf("Error connecting to postgres: \n", err)
	}
}

func createReportDB(dbname string) {
	connectToDB("postgres")
	defer dbConn.Close()
	if dbExists(dbname) {
		LOG.Debugf("Droping db\n")
		execSQL("DROP DATABASE " + dbname)
	}
	LOG.Debugf("Creating DB\n")
	execSQL("CREATE DATABASE " + dbname)
}

func dropReportDB(dbname string) {
	connectToDB("postgres")
	defer dbConn.Close()
	if dbExists(dbname) {
		LOG.Debugf("Droping db\n")
		execSQL("DROP DATABASE " + dbname)
	}
}

func generateDBName() string {
	p, _ := os.Getwd()
	LOG.Debugf("CWD is: %s", p)
	dirs := strings.Split(p, string(os.PathSeparator))
	srPattern := regexp.MustCompile("^[0-9]+$")
	for _, s := range dirs {
		LOG.Debugf("Path element: %s\n", s)
		if srPattern.MatchString(s) {
			ds := time.Now().Format("_20060102")
			return "sr_" + s + ds
		}
	}
	LOG.Debugf("Unable to file SR directory name\n")
	ds := time.Now().Format("_20060102_1504")
	return "unknown_oom" + ds
}

func buildRptFor(segment string, timestamps []string) *SegmentRpt {
	segRpt := SegmentRpt{SegID: segment}
	for _, tss := range timestamps {
		ts, err := time.Parse("060102150405", tss)
		if err != nil {
			LOG.Fatalf("Error parsing time stamp: %v\n", err)
		}
		oomRpt := OOMEventRpt{Timestamp: ts}

		tablename := pq.QuoteIdentifier(fmt.Sprintf("%s_memlog_%s", segment, tss))

		sessions := getSessionMemoryConsumers(tablename)
		for _, sess := range sessions {
			accts := getAccountMemoryConsumption(tablename, sess)
			sessRpt := SessionRpt{sess, accts}
			oomRpt.Sessions = append(oomRpt.Sessions, sessRpt)
		}
		segRpt.OOMEvents = append(segRpt.OOMEvents, oomRpt)
	}
	return &segRpt
}

func processFilesAndGenerateReport(filenames map[string][]string) error {
	dbname := generateDBName()
	createReportDB(dbname)
	connectToDB(dbname)
	//defer dbConn.Close()
	loadFiles(dbname, filenames)

	var segs []string
	for k := range filenames {
		segs = append(segs, k)
	}
	sort.Strings(segs)

	for _, seg := range segs {
		fns := filenames[seg]
		sort.Strings(fns)
		for i := range fns {
			fns[i] = path.Base(fns[i])[:12]
		}
		segRpt := buildRptFor(seg, fns)
		printRpt(*segRpt)
	}
	dbConn.Close()
	dropReportDB(dbname)
	return nil
}

func copyValsFromFile(seg string, filename string) string {

	var table string = seg + "_memlog_" + path.Base(filename)[:12]
	txn, err := dbConn.Begin()
	LOG.Debugf("Created Transaction")
	if err != nil {
		LOG.Fatalf("Error starting transaction: %v\n", err)
	}
	//if _, err := txn.Exec(fmt.Sprintf(ddlString, table)); err != nil {
	if _, err := txn.Exec(fmt.Sprintf(ddlString, pq.QuoteIdentifier(table))); err != nil {
		LOG.Fatalf("Unable to create table: %s\nError: %v\n", table, err)
	}
	LOG.Debugf("Created table: %s", table)
	file, err := os.Open(filename)
	if err != nil {
		LOG.Fatalf("Unable to open memory accounting file: %s. Error: %v\n", filename, err)
	}
	defer file.Close()
	//stmt, err := txn.Prepare(pq.CopyIn(table, copyCols...))
	stmt, err := txn.Prepare(pq.CopyIn(table, copyCols...))
	if err != nil {
		LOG.Fatalf("Unable to prepare copy statement: %v\n", err)
	}
	LOG.Debugf("Prepared COPY statement")
	csvReader := csv.NewReader(file)
	for {
		line, error := csvReader.Read()
		if error != nil {
			if error == io.EOF {
				break
			} else {
				LOG.Fatalf("Problem reading CSV file: %s.  Error: %v\n", filename, error)
			}
		}
		vals := make([]interface{}, len(line))
		for i, v := range line {
			vals[i] = v
		}
		if _, e := stmt.Exec(vals...); e != nil {
			LOG.Fatalf("Error executing copy statement: %v\n", e)
		}
	}
	if _, e := stmt.Exec(); e != nil {
		LOG.Fatalf("Error completing import: %v\n", e)
	}
	if e := stmt.Close(); e != nil {
		LOG.Fatalf("Error closing prepared statement: %v\n", e)
	}
	if e := txn.Commit(); e != nil {
		LOG.Fatalf("Error committing transaction: %v\n", e)
	}
	LOG.Debugf("Finished loading file: %s to table: %s\n", filename, table)
	return table

}

func loadFiles(dbname string, filenames map[string][]string) []string {
	ret := []string{}
	for seg, files := range filenames {
		for _, f := range files {
			LOG.Debugf("Copy values from file: %s\n", f)
			table := copyValsFromFile(seg, f)
			ret = append(ret, table)
		}
	}
	return ret
}

func getSessionMemoryConsumers(tablename string) []SessRow {
	sessQuery := "select logsession, logcmdcount, " +
		"(sum(current)::float / (select sum(current)::float  from " + tablename + " where account_name not in ('Vmem', 'Peak')) * 100) as PctUsed " +
		"from " + tablename + " where account_name not in ('Vmem', 'Peak')   group by 1, 2 order by 3 desc"
	results := GetResults(sessQuery, SessRow{})
	ret := make([]SessRow, 0)
	for i := range results {
		row := results[i].(SessRow)
		if row.PercentMem < 10.0 {
			break
		}
		ret = append(ret, row)
	}
	return ret
}

func getAccountMemoryConsumption(tablename string, sess SessRow) []MemAcctRow {
	acctQuery := "select account_name, " +
		"(sum(current)::float / (select sum(current)::float from " + tablename + " where account_name not in ('Vmem', 'Peak')) * 100) as PctUsed " +
		"from " + tablename + " where account_name not in ('Vmem', 'Peak') and " +
		"logsession = '" + sess.SessID + "' AND logcmdcount = '" + sess.CmdID + "'  group by 1 order by 2 desc"
	results := GetResults(acctQuery, MemAcctRow{})
	ret := make([]MemAcctRow, 0)
	for i := range results {
		row := results[i].(MemAcctRow)
		if row.PercentMem < 10.0 {
			break
		}
		ret = append(ret, row)
	}
	return ret
}

func GetResults(query string, rowType interface{}) []interface{} {
	var values []interface{}
	ret := []interface{}{}
	rows, err := dbConn.Query(query)
	if err != nil {
		LOG.Fatalf("Problem running query to analyze OOM error: %v\n", err)
	}
	defer rows.Close()
	getColumns, cerr := rows.Columns()
	if cerr != nil {
		LOG.Fatalf("Unable to get number of columns: %v\n", err)
	}
	for i := 0; i < len(getColumns); i++ {
		var b []byte
		values = append(values, &b)
	}
	for rows.Next() {
		newObj := reflect.New(reflect.TypeOf(rowType)).Elem()
		err := rows.Scan(values...)
		if err != nil {
			LOG.Fatalf("Unable to create new instance of struct: %v\n", err)
		}
		for i := 0; i < newObj.NumField(); i++ {
			f := newObj.Field(i)
			v := *values[i].(*[]byte)
			vs := string(v[:])
			switch f.Type().Kind() {
			case reflect.Float32, reflect.Float64:
				toSet, err := strconv.ParseFloat(vs, 64)
				if err != nil {
					LOG.Fatalf("Unable to parse float from db: %v\n", err)
				}
				f.SetFloat(toSet)
			case reflect.Int:
				toSet, ok := strconv.ParseInt(vs, 10, 64)
				if ok != nil {
					LOG.Fatalf("Unable to parse int from db: \n", ok)
				}
				f.SetInt(toSet)
			case reflect.String:
				f.SetString(vs)
			}
		}
		ret = append(ret, newObj.Interface())
	}
	return ret
}
