package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "TEST_TASK_QUEUE"

// SimpleWorkflow is a very basic workflow to test the execution path.
func SimpleWorkflow(ctx workflow.Context, name string) (string, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	logger := workflow.GetLogger(ctx)
	logger.Info("SimpleWorkflow started", "name", name)

	var result string
	err := workflow.ExecuteActivity(ctx, SimpleActivity, name).Get(ctx, &result)
	if err != nil {
		logger.Error("Activity failed.", "Error", err)
		return "", err
	}

	logger.Info("SimpleWorkflow completed.", "result", result)
	return result, nil
}

// SimpleActivity is a basic activity called by the workflow.
func SimpleActivity(ctx context.Context, name string) (string, error) {
	return fmt.Sprintf("Hello, %s!", name), nil
}

func main() {
	// 1. Create Temporal Client
	// The client is a heavyweight object that should be created once per process.
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	// 2. Create and Start Worker
	// This worker polls the TaskQueue and executes workflows/activities.
	w := worker.New(c, TaskQueue, worker.Options{})

	w.RegisterWorkflow(SimpleWorkflow)
	w.RegisterActivity(SimpleActivity)

	// Start worker in a separate goroutine so we can execute a workflow in main
	err = w.Start()
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}
	defer w.Stop() // Ensure worker is stopped on exit
	log.Println("Worker started.")

	// 3. Execute Workflow (Client.ExecuteWorkflow -> gRPC StartWorkflowExecution -> ...)
	workflowOptions := client.StartWorkflowOptions{
		ID:        "test_workflow_id",
		TaskQueue: TaskQueue,
	}

	log.Println("Executing Workflow...")
	we, err := c.ExecuteWorkflow(context.Background(), workflowOptions, SimpleWorkflow, "Temporal")
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	}

	log.Println("Started workflow", "WorkflowID", we.GetID(), "RunID", we.GetRunID())

	// 4. Wait for Workflow Completion
	var result string
	err = we.Get(context.Background(), &result)
	if err != nil {
		log.Fatalln("Unable get workflow result", err)
	}
	log.Println("Workflow result:", result)
}
