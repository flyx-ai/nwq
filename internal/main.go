package main

import (
	"context"
	"time"

	"github.com/flyx-ai/nwq/client"
	"github.com/flyx-ai/nwq/internal/testtasks"
	"github.com/flyx-ai/nwq/worker"
	"github.com/nats-io/nats.go"
)

func main() {
	// nc should already be setup through clients
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}

	err = client.Initialize(context.Background(), nc)
	if err != nil {
		panic(err)
	}

	nwqWorker := worker.NewWorker()
	// all involved tasks must be registered to the worker before it can be started
	nwqWorker.RegisterTask(testtasks.DoThingTask)
	nwqWorker.RegisterTask(testtasks.DoOtherThingTask)
	err = nwqWorker.Start(context.Background())
	if err != nil {
		panic(err)
	}

	time.Sleep(time.Hour)
}
