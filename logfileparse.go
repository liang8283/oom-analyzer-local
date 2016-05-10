package main

import (
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// Default time format for GPDB log files
const tsFormat = "2006-01-02 15:04:05.000000 MST"
const workFileDir = "oom_analyzer"

func setupTempFiles() {
	if _, err := os.Stat(workFileDir); os.IsNotExist(err) {
		if e := os.Mkdir(workFileDir, 0777); e != nil {
			LOG.Fatalf("Fatal problem creating work directory %s.  Error %v\n", workFileDir, e)
		}
	}
}

func openOOMEventFile(logtime time.Time, segment string) *os.File {
	fn := logtime.Format("060102150405_" + segment + ".csv")
	fileLoc := filepath.Join(".", workFileDir, fn)
	ret, err := os.Create(fileLoc)
	if err != nil {
		LOG.Fatalf("Fatal problem creating OOM event file. %v\n", err)
	}
	return ret
}

func mergeMaps(dest map[string][]string, source map[string][]string) {
	for k, v := range source {
		if val, ok := dest[k]; ok {
			dest[k] = append(val, v...)
		} else {
			dest[k] = v
		}

	}
}

func processDirectory(directory string) map[string][]string {
	retMap := map[string][]string{}
	walkFn := func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && isCSVFile(path) {
			m, _ := ExtractMemAccounting(path)
			mergeMaps(retMap, m)
		}
		return nil
	}
	filepath.Walk(directory, walkFn)
	return retMap
}

func isDirectory(filepath string) bool {
	f, err := os.Open(filepath)
	if err != nil {
		LOG.Fatalf("Cannot open directory: %s. %v\n", filepath, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		LOG.Fatalf("Poblem with file stats.  Exiting.  %v\n", err)
	}
	return fi.Mode().IsDir()
}

func isCSVFile(file string) bool {
	if filepath.Ext(file) == ".csv" {
		return true
	}
	return false
}

func isGlob(fn string) bool {
	pattern := regexp.MustCompile(".*[\\*\\?\\[\\]]+.*")
	return pattern.MatchString(fn)
}

func processGlob(glob string) map[string][]string {
	fns, err := filepath.Glob(glob)
	if err != nil {
		LOG.Fatalf("Bad glob pattern: %s\n", glob)
	}
	return ProcessFiles(fns)
}

func ProcessFiles(files []string) map[string][]string {
	LOG.Infof("Processing files.  This may take several minutes depending on size and number\n")
	results := map[string][]string{}
	setupTempFiles()
	for _, fn := range files {
		if isGlob(fn) {
			mergeMaps(results, processGlob(fn))
			continue
		}
		if isDirectory(fn) {
			mergeMaps(results, processDirectory(fn))
			continue
		}
		if isCSVFile(fn) {
			m, _ := ExtractMemAccounting(fn)
			mergeMaps(results, m)
			continue
		}
	}
	return results
}

func ExtractMemAccounting(filename string) (map[string][]string, error) {
	var filenames = map[string][]string{}
	iterTime := time.Date(2000, time.January, 01, 0, 0, 0, 0, time.UTC)
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	csvReader := csv.NewReader(f)
	accPattern := regexp.MustCompile("^memory: .*")
	var csvWriter *csv.Writer
	for {
		line, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}
		if accPattern.MatchString(line[18]) {
			logTime, _ := time.Parse(tsFormat, line[0])
			diff := logTime.Sub(iterTime)
			if diff.Minutes() > 1 {
				seg := line[11]
				oFile := openOOMEventFile(logTime, line[11])
				defer oFile.Close()
				csvWriter = csv.NewWriter(oFile)
				defer csvWriter.Flush()
				if val, ok := filenames[seg]; ok {
					filenames[seg] = append(val, oFile.Name())
				} else {
					filenames[seg] = []string{oFile.Name()}
				}

			}
			iterTime = logTime
			acctEntries := strings.Split(line[18][8:], ",")
			for index, val := range acctEntries {
				acctEntries[index] = strings.TrimSpace(val)
			}
			if acctEntries[0] == "account_name" {
				continue
			}
			outLine := append(line[:18], acctEntries...)
			csvWriter.Write(outLine)
		}

	}
	return filenames, nil
}
