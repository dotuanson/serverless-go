package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"serverless/athena"
)

func main() {
	lambda.Start(athena.CronjobReport)
}
