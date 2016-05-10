package main


var descriptions = map[string]string{
	"Deserializer" : "Memory used to represent the query plan.  Results in OOM if query plan is too large.  Please set max_plan_size",
	"Executor" : "Memory used by the top level of query execution.  May consume more mememory if many thousand subplans generated.\n\t If this is source of OOM please consult with SME and Development",
	"Main" : "Main postgres memory context.  OOM here needs to be addressed by Development",
	"MemAcct" : "Memory used to track memory accounting.  OOM in this needs to be addressed by Development",
	"Planner" : "Memory used by planner.  Should never be an issue during normal execution.  OOM here needs to be referred to Development",
	"Rollover" : "Memory that accumulates during mutltiple query runs.  If source of OOM check that the command count is high.\n\tIf high, then sessions need to be closed.  If command count is low ( < 100) then we need to reproduce with workload for possible bug",
	"Root" : "Root memory Conects.  Any issues here must be send to Development",
	"SharedHeader" : "Should never see.  Please engage with SME and Development",
	"Top" : "Catchall top level account.  Please refer to SME and Development for any issues",
	"Parser" : "Memory used for parsing sql queries.  Should not be seend during normal execution on segments.\n\t If found on master, SQL needs to be reviewed and possibly referred to Development",
	"Optimizer" : "Memory context used for PQO.  OOM here please refer to Development",
	"Dispatcher" : "Memory used by dispatch.  Any issues should be reffered to Development",
	"Serializer" : "Memory needed to serialize a plan.  Any issues please refer to Development",
	"X_AOCSCAN" : "Unknown.  Please engage with SME for further investigation",
	"X_Agg" : "Unknown.  Please engage with SME for further investigation",
	"X_Alien" : "Memory used by execution nodes not used by the slice in question.  Results in OOM if query plan is toolarge and has too many slices",
	"X_Append" : "Unknown.  Please engage with SME for further investigation",
	"X_AppendOnlyScan" : "Unknown.  Please engage with SME for further investigation",
	"X_AssertOp" : "Unknown.  Please engage with SME for further investigation",
	"X_Bitmap" : "Unknown.  Please engage with SME for further investigation",
	"X_BitmapAppendOnlyScan" : "Unknown.  Please engage with SME for further investigation",
	"X_BitmapHeapScan" : "Unknown.  Please engage with SME for further investigation",
	"X_BitmapIndexScan" : "Unknown.  Please engage with SME for further investigation",
	"X_BitmapOr" : "Unknown.  Please engage with SME for further investigation",
	"X_BitmapTableScan" : "Unknown.  Please engage with SME for further investigation",
	"X_DML" : "Unknown.  Please engage with SME for further investigation",
	"X_DynamicIndexScan" : "Unknown.  Please engage with SME for further investigation",
	"X_DynamicTableScan" : "Unknown.  Please engage with SME for further investigation",
	"X_ExternalScan" : "Unknown.  Please engage with SME for further investigation",
	"X_FunctionScan" : "Unknown.  Please engage with SME for further investigation",
	"X_Hash" : "Unknown.  Please engage with SME for further investigation",
	"X_HashJoin" : "Likely due to a known issue with hash join.  No immediate solution other than to redesign query to reduce number of tubples being process.\n\t Please try materializing the sub-queries and breaking query into 2 parts",
	"X_IndexScan" : "Unknown.  Please engage with SME for further investigation",
	"X_Limit" : "Unknown.  Please engage with SME for further investigation",
	"X_Material" : "Unknown.  Please engage with SME for further investigation",
	"X_MergeJoin" : "Unknown.  Please engage with SME for further investigation",
	"X_Motion" : "Unknown.  Please engage with SME for further investigation",
	"X_NestLoop" : "Unknown.  Please engage with SME for further investigation",
	"X_PartitionSelector" : "Unknown.  Please engage with SME for further investigation",
	"X_Repeat" : "Unknown.  Please engage with SME for further investigation",
	"X_Result" : "Unknown.  Please engage with SME for further investigation",
	"X_RowTrigger" : "Unknown.  Please engage with SME for further investigation",
	"X_SeqScan" : "Unknown.  Please engage with SME for further investigation",
	"X_Sequence" : "Unknown.  Please engage with SME for further investigation",
	"X_SetOp" : "Unknown.  Please engage with SME for further investigation",
	"X_ShareInputScan" : "Unknown.  Please engage with SME for further investigation",
	"X_Sort" : "Unknown.  Please engage with SME for further investigation",
	"X_SplitUpdate" : "Unknown.  Please engage with SME for further investigation",
	"X_SubqueryScan" : "Unknown.  Please engage with SME for further investigation",
	"X_TableFunctionScan" : "Unknown.  Please engage with SME for further investigation",
	"X_TableScan" : "Unknown.  Please engage with SME for further investigation",
	"X_TidScan" : "Unknown.  Please engage with SME for further investigation",
	"X_Unique" : "Unknown.  Please engage with SME for further investigation",
	"X_ValuesScan" : "Unknown.  Please engage with SME for further investigation",
	"X_Window" : "Unknown.  Please engage with SME for further investigation",
}

	

	

func printRpt(segRpt SegmentRpt) {
	LOG.Infof("OOM Report for segment: %s\n", segRpt.SegID)
	for _, oomRpt := range segRpt.OOMEvents {
		LOG.Infof("OOM Event Timestamp: %s\n", oomRpt.Timestamp.String())
		for _, sessRpt := range oomRpt.Sessions {
			LOG.Infof("  Session ID: %s\n", sessRpt.SessID)
			LOG.Infof("  Command: %s\n", sessRpt.CmdID)
			LOG.Infof("  VMmem Used: %.2f%%\n\n", sessRpt.PercentMem)
			for _, acct := range sessRpt.MemAccounts {
				LOG.Infof("    Account Name: %s\n", acct.AccountName)
				LOG.Infof("    VMem Used: %.2f%%\n\n", acct.PercentMem)
				if descr, ok := descriptions[acct.AccountName]; ok {
					LOG.Infof("    Description: %s\n", descr)
				} else {
					LOG.Infof("    Description: None found.  Please consult with SME\n")
				}
			}
			LOG.Infof("\n")
		}
	}
}
