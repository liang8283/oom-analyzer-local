package main

import (
	"github.com/pivotal-gss/utils/mlogger"
	"os"
)

var LOG mlogger.Mlogger

func main() {
	mlog, _ := mlogger.NewStdoutOnlyLogger()
	LOG = mlog
	//	LOG.EnableDebug()
	if len(os.Args) <= 1 {
		LOG.Fatalf("Please sepcify at least one file name.\n")
	}
	filenames := os.Args[1:]
	fns := ProcessFiles(filenames)
	if len(fns) == 0 {
		LOG.Warnf("No out of memory events found\n")
	}
	LOG.Debugf("Filenames: %s\n", fns)
	processFilesAndGenerateReport(fns)
	dropReportDB(generateDBName())
	if _, err := os.Stat(workFileDir); err != nil {
		if os.IsNotExist(err) == false {
			LOG.Fatalf("Unable to remove working directory. %v\n\n", err)
		}
	} else {
		if err := os.RemoveAll(workFileDir); err != nil {
			LOG.Fatalf("Error deleting working directory: %v\n\n", err)
		}
	}

}
