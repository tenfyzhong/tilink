package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/tenfyzhong/tilink/internal/worker"
)

func main() { lambda.Start(worker.Handle) }
