package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"io"
	"log"
	"net/http"
	"time"
)

const (
	REGION        = ""
	OUTPUT_BUCKET = ""
	TOKEN         = ""
)

func generateQueryResponseTime(startTime, endTime string) string {
	return fmt.Sprintf(``, startTime, endTime)
}

func generateQueryStatusCodes(startTime, endTime string) string {
	return fmt.Sprintf(``, startTime, endTime)
}

func getCurrentTimeUtc() (time.Time, error) {
	location, err := time.LoadLocation("Asia/Bangkok")
	if err != nil {
		return time.Time{}, err
	}
	currentTimeUTC7 := time.Now().In(location)
	currentTimeUTC := currentTimeUTC7.UTC()

	return currentTimeUTC, nil
}

/**
 * Submits a sample query to Athena and returns the execution ID of the query.
 */
func submitAthenaQuery(query string, outputBucket string, client *athena.Athena) (string, error) {
	// The result configuration specifies where the results of the query should go in S3 and encryption options
	resultConf := &athena.ResultConfiguration{
		OutputLocation: aws.String(outputBucket),
	}
	// The Parameters passed into the Execution Request which includes the query string and the Output Location.
	params := &athena.StartQueryExecutionInput{
		QueryString:         aws.String(query),
		ResultConfiguration: resultConf,
	}

	req, resp := client.StartQueryExecutionRequest(params)

	err := req.Send()
	if err != nil {
		fmt.Printf("StartQueryExecutionRequest error is : %v\n", err)
		return "", err
	}
	return *resp.QueryExecutionId, nil
}

/**
 * Wait for an Athena query to complete, fail or to be cancelled. This is done by polling Athena over an
 * interval of time. If a query fails or is cancelled, then it will throw an exception.
 */
func awaitAthenaQuery(executionID string, client *athena.Athena) error {
	params2 := &athena.GetQueryExecutionInput{
		QueryExecutionId: &executionID,
	}
	duration := time.Duration(2) * time.Second // Pause for 2 seconds

	for {
		executionOutput, err := client.GetQueryExecution(params2)
		if err != nil {
			return err
		}
		log.Println(*executionOutput.QueryExecution.Status.State)
		if *executionOutput.QueryExecution.Status.State != "QUEUED" && *executionOutput.QueryExecution.Status.State != "RUNNING" {
			log.Println("Query is complete!")
			if *executionOutput.QueryExecution.Status.State == "FAILED" {
				return fmt.Errorf("Query encountered an error: %v", *executionOutput.QueryExecution.Status.StateChangeReason)
			}
			break
		}
		log.Println("Waiting for query to finish...")
		time.Sleep(duration)
	}
	return nil
}

/**
 * This code calls Athena and retrieves the results of a query.
 * The query must be in a completed state before the results can be retrieved and
 * paginated.
 */
func processAthenaResults(executionID string, client *athena.Athena) (string, error) {
	var input athena.GetQueryResultsInput
	input.SetQueryExecutionId(executionID)

	results, err := client.GetQueryResults(&input)
	if err != nil {
		return "", err
	}

	var text string
	for idx, row := range results.ResultSet.Rows {
		if idx == 0 {
			text = fmt.Sprintf("* | %s | %s |\n", *row.Data[0].VarCharValue, *row.Data[1].VarCharValue)
			continue
		}
		text += fmt.Sprintf("  + %s: %s requests\n", *row.Data[0].VarCharValue, *row.Data[1].VarCharValue)
	}
	return text, nil
}

func alertTelegram(currentTimeUtc string, texts ...string) error {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", TOKEN)
	var text = fmt.Sprintf("✍️ [%s] Report STT\n\n", currentTimeUtc)

	for _, txt := range texts {
		text = fmt.Sprintf("%s%s\n", text, txt)
	}
	body, _ := json.Marshal(map[string]string{
		"chat_id": "",
		"text":    text,
	})
	res, err := http.Post(
		url,
		"application/json",
		bytes.NewBuffer(body),
	)
	defer res.Body.Close()

	// Check the HTTP status code
	if res.StatusCode != http.StatusOK {
		return err
	}

	// Read the response body
	body, err = io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	return nil
}

func workFlow(client *athena.Athena, query string) (string, error) {
	strExecutionID, err := submitAthenaQuery(query, OUTPUT_BUCKET, client)
	fmt.Println(query)
	if err != nil {
		return "", err
	}

	err = awaitAthenaQuery(strExecutionID, client)
	if err != nil {
		return "", err
	}

	text, err := processAthenaResults(strExecutionID, client)
	if err != nil {
		return "", err
	}

	return text, nil
}

func cronjobReport(ctx context.Context) error {
	config := &aws.Config{}
	config.WithRegion(REGION)

	// Create the session that the service will use.
	sess := session.Must(session.NewSession(config))

	// Build an AthenaClient client
	client := athena.New(sess, aws.NewConfig().WithRegion(REGION))

	// Athena query
	var err error
	currentTimeUtc, err := getCurrentTimeUtc()
	if err != nil {
		return err
	}
	var (
		startOfDay       = time.Date(currentTimeUtc.Year(), currentTimeUtc.Month(), currentTimeUtc.Day(), 0, 0, 0, 0, time.UTC)
		endOfDay         = time.Date(currentTimeUtc.Year(), currentTimeUtc.Month(), currentTimeUtc.Day(), 23, 59, 59, 999999999, time.UTC)
		startOfDayFormat = startOfDay.Format("2006-01-02T15:04:05.000Z")
		endOfDayFormat   = endOfDay.Format("2006-01-02T15:04:05.000Z")
	)
	var textResponseTime, textStatusCode string
	if textResponseTime, err = workFlow(client, generateQueryResponseTime(startOfDayFormat, endOfDayFormat)); err != nil {
		return err
	}
	if textStatusCode, err = workFlow(client, generateQueryStatusCodes(startOfDayFormat, endOfDayFormat)); err != nil {
		return err
	}
	err = alertTelegram(time.Now().Format("2006-01-02"), textResponseTime, textStatusCode)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	lambda.Start(cronjobReport)
}
