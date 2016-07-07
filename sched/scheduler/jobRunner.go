package scheduler

import (
	"fmt"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	cm "github.com/scootdev/scoot/sched/clustermembership"
	dist "github.com/scootdev/scoot/sched/distributor"
	"sync"
)

const MAX_RETRY = 3

type jobRunner struct {
	job              sched.Job
	saga             saga.Saga
	sagaId           string
	initialSagaState *saga.SagaState
	dist             *dist.PoolDistributor
	wg               sync.WaitGroup
}

func NewJobRunner(job sched.Job, saga saga.Saga, sagaState *saga.SagaState, nodes []cm.Node) jobRunner {
	jr := jobRunner{
		job:              job,
		saga:             saga,
		sagaId:           sagaState.SagaId(),
		initialSagaState: sagaState,
		dist:             dist.NewPoolDistributor(cm.StaticClusterFactory(nodes)),
	}

	//go jr.updateSagaState()
	return jr
}

// Runs the Job associated with this JobRunner to completion
func (jr jobRunner) runJob() {
	// don't re-run an already completed saga
	if jr.initialSagaState.IsSagaCompleted() {
		return
	}

	// if the saga has not been aborted, just run each task in the saga
	// TODO: this can be made smarter to run the next Best Task instead of just
	// the next one in order etc....
	if !jr.initialSagaState.IsSagaAborted() {
		for _, task := range jr.job.Tasks {

			if !jr.initialSagaState.IsTaskCompleted(task.Id) {
				node := jr.dist.ReserveNode()

				// Put StartTask Message on SagaLog
				_, err := jr.saga.StartTask(jr.sagaId, task.Id, nil)
				if err != nil {
					handleSagaLogErrors(err)
				}

				jr.wg.Add(1)
				go func(node cm.Node, task sched.Task) {
					defer jr.dist.ReleaseNode(node)
					defer jr.wg.Done()

					// TODO: After a number of attempts we should stop
					// Trying to run a task, could be a poison pill
					// Implement deadletter queue
					taskExecuted := false
					for !taskExecuted {
						err := node.SendMessage(task)
						if err == nil {
							taskExecuted = true
						}
					}

					_, err := jr.saga.EndTask(jr.sagaId, task.Id, nil)
					if err != nil {
						handleSagaLogErrors(err)
					}

				}(node, task)
			}
		}
	} else {
		// TODO: we don't have a way to specify comp tasks yet
		// Once we do they should be ran here.  Currently the
		// scheduler only supports ForwardRecovery in Sagas so Panic!
		panic("Rollback Recovery Not Supported Yet!")
	}

	// wait for all tasks to complete
	jr.wg.Wait()

	// Log EndSaga Message to SagaLog
	_, err := jr.saga.EndSaga(jr.sagaId)
	if err != nil {
		handleSagaLogErrors(err)
	}

	return
}

func handleSagaLogErrors(err error) {
	if retryableErr(err) {
		// TODO: Implement deadletter queue.  SagaLog is failing to store this message some reason,
		// Could be bad message or could be because the log is unavailable.  Put on Deadletter Queue and Move On
		// For now just panic, for Alpha (all in memory this SHOULD never happen)
		panic(fmt.Sprintf("Failed to succeesfully Write to SagaLog this Job should be put on the deadletter queue.  Err: %v", err))
	} else {
		// Something is really wrong.  Either an Invalid State Transition, or we formatted the request to the SagaLog incorrectly
		// These errors indicate a fatal bug in our code.  So we should panic.
		panic(fmt.Sprintf("Fatal Error Writing to SagaLog.  Err: %v", err))
	}
}

// checks the error returned my updating saga state.
func retryableErr(err error) bool {

	switch err.(type) {
	// InvalidSagaState is an unrecoverable error. This indicates a fatal bug in the code
	// which is asking for an impossible transition.
	case saga.InvalidSagaStateError:
		return false

	// InvalidSagaMessage is an unrecoverable error.  This indicates a fatal bug in the code
	// which is applying invalid parameters to a saga.
	case saga.InvalidSagaMessageError:
		return false

	// InvalidRequestError is an unrecoverable error.  This indicates a fatal bug in the code
	// where the SagaLog cannot durably store messages
	case saga.InvalidRequestError:
		return false

	// InternalLogError is a transient error experienced by the log.  It was not
	// able to durably store the message but it is ok to retry.
	case saga.InternalLogError:
		return true

	// unknown error, default to retryable.
	default:
		return true
	}
}
