package fileprocessing

import (
	"log"
	"time"

	"go.temporal.io/sdk/temporal"

	"go.temporal.io/sdk/workflow"
)

// SampleFileProcessingWorkflow workflow definition
func SampleFileProcessingWorkflow(ctx workflow.Context, fileName string) (err error) {
	log.Printf("SampleFileProcessingWorkflow: start, '%s'", fileName)
	ao := workflow.ActivityOptions{
		TaskQueue: "chickens",
		StartToCloseTimeout: time.Minute,
		HeartbeatTimeout:    time.Second * 2, // such a short timeout to make sample fail over very fast
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Retry the whole sequence from the first activity on any error
	// to retry it on a different host. In a real application it might be reasonable to
	// retry individual activities as well as the whole sequence discriminating between different types of errors.
	// See the retryactivity sample for a more sophisticated retry implementation.
	for i := 1; i < 5; i++ {
		log.Printf("SampleFileProcessingWorkflow: processing file %d", i)
		err = processFile(ctx, fileName)
		if err == nil {
			log.Printf("SampleFileProcessingWorkflow: success")
			break
		}
	}
	if err != nil {
		workflow.GetLogger(ctx).Error("Workflow failed.", "Error", err.Error())
	} else {
		workflow.GetLogger(ctx).Info("Workflow completed.")
	}
	return err
}

func processFile(ctx workflow.Context, fileName string) (err error) {
	log.Printf("processFile: start: '%s'", fileName)
	so := &workflow.SessionOptions{
		CreationTimeout:  time.Minute,
		ExecutionTimeout: time.Minute,
	}
	sessionCtx, err := workflow.CreateSession(ctx, so)
	if err != nil {
		return err
	}
	defer workflow.CompleteSession(sessionCtx)

	var downloadedName string
	var a *Activities
	log.Printf("processFile: executing DownloadFileActivity")
	err = workflow.ExecuteActivity(sessionCtx, a.DownloadFileActivity, fileName).Get(sessionCtx, &downloadedName)
	if err != nil {
		return err
	}

	var processedFileName string
	log.Printf("processFile: executing ProcessFileActivity")
	err = workflow.ExecuteActivity(sessionCtx, a.ProcessFileActivity, downloadedName).Get(sessionCtx, &processedFileName)
	if err != nil {
		return err
	}

	log.Printf("processFile: executing UploadFileActivity")
	err = workflow.ExecuteActivity(sessionCtx, a.UploadFileActivity, processedFileName).Get(sessionCtx, nil)
	log.Printf("processFile: exit")
	return err
}
