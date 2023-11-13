package athena

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"io"
	"log"
	"net/http"
	"serverless/util"
	"time"
)

func generateQueryResponseTime(startTime, endTime string) string {
	return fmt.Sprintf(`WITH time_intervals AS (
			  SELECT
				CASE
			      WHEN target_processing_time >= 0 AND target_processing_time <= 0.8 THEN '0-3 seconds'
				  WHEN target_processing_time > 0.8 AND target_processing_time <= 3 THEN '0-3 seconds'
				  WHEN target_processing_time > 3 AND target_processing_time <= 5 THEN '3-5 seconds'
				  ELSE '>5 seconds'
				END AS time_interval
			  FROM s3_albvaenginelogs_db.s3_alb_va_engine_logs
			  WHERE request_url = 'https://apis-new.vastt.vnlp.ai:443/transform-service/va/stt?noiseDetection=true'
			    AND (client_ip = '124.158.11.112' OR client_ip = '119.82.135.12')
			    AND elb_status_code = 200
			    AND (time >= '%s' AND time <= '%s')
			)
			SELECT time_interval, COUNT(*) AS count
			FROM time_intervals
			GROUP BY time_interval
			ORDER BY time_interval`, startTime, endTime)
}

func generateQueryStatusCodes(startTime, endTime string) string {
	return fmt.Sprintf(`SELECT elb_status_code, COUNT(elb_status_code) AS total_request
			FROM s3_albvaenginelogs_db.s3_alb_va_engine_logs
			WHERE request_url = 'https://apis-new.vastt.vnlp.ai:443/transform-service/va/stt?noiseDetection=true'
			  AND (client_ip = '124.158.11.112' OR client_ip = '119.82.135.12')
			  AND (time >= '%s' AND time <= '%s')
			GROUP BY elb_status_code
			ORDER BY elb_status_code ASC`, startTime, endTime)
}

func getCurrentTimeUtc() time.Time {
	currentTimeUTC := time.Now().UTC()
	return currentTimeUTC
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

func alertTelegram(configApp util.Config, currentTimeUtc string, texts ...string) error {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", configApp.TelegramToken)
	var text = fmt.Sprintf("✍️ [%s] Report STT\n\n", currentTimeUtc)

	for _, txt := range texts {
		text = fmt.Sprintf("%s%s\n", text, txt)
	}
	body, _ := json.Marshal(map[string]string{
		"chat_id": configApp.TelegramChatID,
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
	_, err = io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	return nil
}

func workFlow(configApp util.Config, client *athena.Athena, query string) (string, error) {
	strExecutionID, err := submitAthenaQuery(query, configApp.OutputBucket, client)
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

func CronjobReport(ctx context.Context) error {
	//configApp, err := util.LoadConfig("./")
	//if err != nil {
	//	log.Fatal("cannot load config:", err)
	//}
	var err error
	configApp := util.Config{
		Region:         "ap-southeast-1",
		OutputBucket:   "s3://aws-athena-emandai/REPORTS",
		TelegramToken:  "",
		TelegramChatID: "",
	}

	configAWS := &aws.Config{}
	configAWS.WithRegion(configApp.Region)

	// Create the session that the service will use.
	sess := session.Must(session.NewSession(configAWS))

	// Build an AthenaClient client
	client := athena.New(sess, aws.NewConfig().WithRegion(configApp.Region))

	// Athena query
	currentTimeUtc := getCurrentTimeUtc()
	var (
		startOfDay       = time.Date(currentTimeUtc.Year(), currentTimeUtc.Month(), currentTimeUtc.Day(), 0, 0, 0, 0, time.UTC)
		endOfDay         = time.Date(currentTimeUtc.Year(), currentTimeUtc.Month(), currentTimeUtc.Day(), 17, 0, 0, 0, time.UTC)
		startOfDayFormat = startOfDay.Format("2006-01-02T15:04:05.000Z")
		endOfDayFormat   = endOfDay.Format("2006-01-02T15:04:05.000Z")
	)
	var textResponseTime, textStatusCode string
	if textResponseTime, err = workFlow(configApp, client, generateQueryResponseTime(startOfDayFormat, endOfDayFormat)); err != nil {
		log.Printf("err: %s", err)
		return err
	}
	if textStatusCode, err = workFlow(configApp, client, generateQueryStatusCodes(startOfDayFormat, endOfDayFormat)); err != nil {
		log.Printf("err: %s", err)
		return err
	}
	err = alertTelegram(configApp, time.Now().Format("2006-01-02"), textResponseTime, textStatusCode)
	if err != nil {
		log.Printf("err: %s", err)
		return err
	}
	return nil
}
