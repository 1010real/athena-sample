package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
)

// todo: replace output location (one of S3 URI)
const OUTPUT_LOCATION = "s3://xxxxx"

// todo: replace Athena Query (SQL for Athena)
var queryString = strings.Join([]string{
	"SELECT *",
	"FROM \"DATABASE_NAME_FOR_ATHENA\".\"TABLE_NAME_FOR_ATHENA\"",
	"LIMIT 100;"}, " ")

var (
	client *athena.Athena
)

func main() {
	// setting up Athena client
	client = initAthenaClient()

	resultConf := &athena.ResultConfiguration{
		OutputLocation: aws.String(OUTPUT_LOCATION),
	}

	input := &athena.StartQueryExecutionInput{
		QueryString:         &queryString,
		ResultConfiguration: resultConf,
	}

	// athenaクエリ実行
	sqeOutput, err := client.StartQueryExecution(input)
	if err != nil {
		fmt.Println(err.Error())
	}

	// 実行完了を待つ（ステータスを監視）
	executionInput := &athena.GetQueryExecutionInput{
		QueryExecutionId: sqeOutput.QueryExecutionId,
	}
L:
	for {
		gqeOutput, err := client.GetQueryExecution(executionInput)
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Printf("%s\n", *gqeOutput.QueryExecution.Status.State) // for debug
		// https://docs.aws.amazon.com/sdk-for-go/api/service/athena/#pkg-consts
		switch *gqeOutput.QueryExecution.Status.State {
		case athena.QueryExecutionStateQueued, athena.QueryExecutionStateRunning:
			time.Sleep(1 * time.Second)
		case athena.QueryExecutionStateSucceeded:
			break L
		case athena.QueryExecutionStateFailed, athena.QueryExecutionStateCancelled:
		default:
			fmt.Println(errors.New(gqeOutput.String()))
		}
	}

	var (
		token     *string = nil
		maxResult int64   = 50
	)

	for {
		gqrinput := &athena.GetQueryResultsInput{MaxResults: &maxResult, NextToken: token, QueryExecutionId: sqeOutput.QueryExecutionId}

		results, err := client.GetQueryResults(gqrinput)
		if err != nil {
			fmt.Println(err.Error())
		}

		parsedResults, err := parseResults(results, token)
		if err != nil {
			fmt.Println(err.Error())
		}

		// do something for parsedResults
		for _, v := range parsedResults {
			fmt.Println(v)
		}

		// NextTokenがnilなら終了（全結果取得済み）
		token = results.NextToken
		if token == nil {
			break
		}
	}
}

func initAthenaClient() *athena.Athena {
	cred := credentials.NewStaticCredentials(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		"",
	)
	conf := aws.Config{
		Region:      aws.String("ap-northeast-1"),
		Credentials: cred,
	}
	sess := session.Must(session.NewSession(&conf))
	return athena.New(sess)
}

type RowData map[string]string

func parseResults(res *athena.GetQueryResultsOutput, token *string) ([]RowData, error) {
	rds := []RowData{}
	rns := make([]string, len(res.ResultSet.ResultSetMetadata.ColumnInfo))
	for i, meta := range res.ResultSet.ResultSetMetadata.ColumnInfo {
		rns[i] = *meta.Name
	}
	for i, row := range res.ResultSet.Rows {
		if i == 0 && token == nil {
			// tokenなし（初回）リクエストの場合、header行が先頭に入ってくるため無視する
			continue
		}
		rd := RowData{}
		for j, data := range row.Data {
			rd[rns[j]] = *data.VarCharValue
		}
		rds = append(rds, rd)
	}
	return rds, nil
}
