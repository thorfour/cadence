// Copyright (c) 2017-2020 Uber Technologies Inc.
// Portions of the Software are attributed to Copyright (c) 2020 Temporal Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cli

import (
	"fmt"
	"time"
	"errors"
	"encoding/json"
	"strconv"

	"github.com/urfave/cli"
	s "go.uber.org/cadence/.gen/go/shared"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/common/codec"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/sql"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/common/quotas"
)

var (

)

func AdminDeleteClosedWorkflows(c *cli.Context) {

	wfClient := getWorkflowClient(c)
	earliestTime := parseTime(c.String(FlagEarliestTime), 0)
	latestTime := parseTime(c.String(FlagLatestTime), time.Now().UnixNano())
	workflowID := c.String(FlagWorkflowID)
	workflowType := c.String(FlagWorkflowType)
	pageSize := c.Int(FlagPageSize)

	//Create sql connection for execution store
	sqlDB := connectToSQL(c)

	if pageSize <= 0 {
		pageSize = 50
	}

	if len(workflowID) > 0 && len(workflowType) > 0 {
		ErrorAndExit(optionErr, errors.New("you can filter on workflow_id or workflow_type, but not on both"))
	}


	historyManager := initializeHistoryStore(c, 50)
	var results []*s.WorkflowExecutionInfo
	var nextPageToken []byte

	for {
		results, nextPageToken = listClosedWorkflow(wfClient, pageSize, earliestTime, latestTime, workflowID, workflowType, getWorkflowStatus("completed"), nextPageToken, c)
		handleWorkflowDeletions(c, results, historyManager, sqlDB)
		if len(nextPageToken) == 0 {
			break
		}
	}

	return
}

func describeWorkflowExecutionMutableState(c *cli.Context, workflowId string, runId string) *types.AdminDescribeWorkflowExecutionResponse {
	adminClient := cFactory.ServerAdminClient(c)
	domain := getRequiredGlobalOption(c, FlagDomain)

	ctx, cancel := newContext(c)
	defer cancel()

	resp, err := adminClient.DescribeWorkflowExecution(ctx, &types.AdminDescribeWorkflowExecutionRequest{
		Domain: domain,
		Execution: &types.WorkflowExecution{
			WorkflowID: workflowId,
			RunID:      runId,
		},
	})
	if err != nil {
		ErrorAndExit("Get workflow mutableState failed", err)
	}
	return resp
}

func handleWorkflowDeletions(c *cli.Context, executions []*s.WorkflowExecutionInfo, historyManager persistence.HistoryManager, sqlDB sqlplugin.DB) {
	for _, execution := range executions {
		e := execution.Execution
		resp := describeWorkflowExecutionMutableState(c, e.GetWorkflowId(), e.GetRunId())
		msStr := resp.GetMutableStateInDatabase()
		ms := persistence.WorkflowMutableState{}
		err := json.Unmarshal([]byte(msStr), &ms)
		if err != nil {
			ErrorAndExit("json.Unmarshal err", err)
		}
		domainID := ms.ExecutionInfo.DomainID
		skipError := c.Bool(FlagSkipErrorMode)
		dryRun := c.Bool(FlagDryRun)

		shardID := resp.GetShardID()
		shardIDInt, err := strconv.Atoi(shardID)
		branchInfo := shared.HistoryBranch{}
		thriftrwEncoder := codec.NewThriftRWEncoder()
		branchTokens := [][]byte{ms.ExecutionInfo.BranchToken}
		if ms.VersionHistories != nil {
			// if VersionHistories is set, then all branch infos are stored in VersionHistories
			branchTokens = [][]byte{}
			for _, versionHistory := range ms.VersionHistories.ToInternalType().Histories {
				branchTokens = append(branchTokens, versionHistory.BranchToken)
			}
		}

		for _, branchToken := range branchTokens {
			err = thriftrwEncoder.Decode(branchToken, &branchInfo)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("deleting history events for ...")
			//prettyPrintJSONObject(branchInfo)
			ctx, cancel := newContext(c)
			defer cancel()
			if !dryRun {
				err := historyManager.DeleteHistoryBranch(ctx, &persistence.DeleteHistoryBranchRequest{
					BranchToken: branchToken,
					ShardID: &shardIDInt,
				})
				if err != nil {
					if skipError {
						fmt.Println("failed to delete history, ", err)
					} else {
						ErrorAndExit("DeleteHistoryBranch err", err)
					}
				}
			} else {
				fmt.Println("would delete: ", branchInfo, e.GetWorkflowId(), e.GetRunId())
			}



		}

		encodingType := c.String(FlagEncodingType)
		decodingTypesStr := c.StringSlice(FlagDecodingTypes)
		var decodingTypes []common.EncodingType
		for _, dt := range decodingTypesStr {
			decodingTypes = append(decodingTypes, common.EncodingType(dt))
		}

		execStore, err := sql.NewSQLExecutionStore(sqlDB, loggerimpl.NewNopLogger(), shardIDInt, getSQLParser(common.EncodingType(encodingType), decodingTypes...))
		//execManager := persistence.NewExecutionManagerImpl(execStore, loggerimpl.NewNopLogger())

		req := &persistence.DeleteWorkflowExecutionRequest{
			DomainID:   domainID,
			WorkflowID: e.GetWorkflowId(),
			RunID:      e.GetRunId(),
		}

		ctx, cancel := newContext(c)
		defer cancel()

		if !dryRun {
			err = execStore.DeleteWorkflowExecution(ctx, req)
			if err != nil {
				if skipError {
					fmt.Println("delete mutableState row failed, ", err)
				} else {
					ErrorAndExit("delete mutableState row failed", err)
				}
			}
			fmt.Println("delete mutableState row successfully")
		} else {
			fmt.Println("would delete execution:", e.GetWorkflowId(), e.GetRunId())
		}


		deleteCurrentReq := &persistence.DeleteCurrentWorkflowExecutionRequest{
			DomainID:   domainID,
			WorkflowID: e.GetWorkflowId(),
			RunID:      e.GetRunId(),
		}
		if !dryRun {
			err = execStore.DeleteCurrentWorkflowExecution(ctx, deleteCurrentReq)
			if err != nil {
				if skipError {
					fmt.Println("delete current row failed, ", err)
				} else {
					ErrorAndExit("delete current row failed", err)
				}
			}
			fmt.Println("delete current row successfully")
		} else {
			fmt.Println("would delete current execution:", e.GetWorkflowId(), e.GetRunId())
		}
	}
}

func printListWorkflowResults( executions []*s.WorkflowExecutionInfo) {
	for _, execution := range executions {
		fmt.Println(convertTime(execution.GetStartTime(), false), anyToString(execution, true, 0) + ",")
	}
}

func initializeHistoryStore(
	c *cli.Context,
	rps int,
) persistence.HistoryManager {

	var execStore persistence.HistoryStore
	dbType := c.String(FlagDBType)
	logger := loggerimpl.NewNopLogger()
	switch dbType {
	case "mysql":
		execStore = initializeSQLHistoryStore(c, logger)
	case "postgres":
		execStore = initializeSQLHistoryStore(c, logger)
	default:
		ErrorAndExit("The DB type is not supported. Options are: mysql, postgres.", nil)
	}

	historyManager := persistence.NewHistoryV2ManagerImpl(execStore, logger, dynamicconfig.GetIntPropertyFn(common.DefaultTransactionSizeLimit))
	rateLimiter := quotas.NewSimpleRateLimiter(rps)
	return persistence.NewHistoryPersistenceRateLimitedClient(historyManager, rateLimiter, logger)
}

func initializeSQLHistoryStore(
	c *cli.Context,
	logger log.Logger,
) persistence.HistoryStore {

	sqlDB := connectToSQL(c)
	encodingType := c.String(FlagEncodingType)
	decodingTypesStr := c.StringSlice(FlagDecodingTypes)
	var decodingTypes []common.EncodingType
	for _, dt := range decodingTypesStr {
		decodingTypes = append(decodingTypes, common.EncodingType(dt))
	}
	execStore, err := sql.NewHistoryV2Store(sqlDB, logger, getSQLParser(common.EncodingType(encodingType), decodingTypes...))
	if err != nil {
		ErrorAndExit("Failed to get execution store from cassandra config", err)
	}
	return execStore
}



