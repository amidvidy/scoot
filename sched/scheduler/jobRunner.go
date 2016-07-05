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
	job       sched.Job
	saga      saga.Saga
	sagaState *saga.SagaState
	dist      *dist.PoolDistributor

	updateChan chan stateUpdate
	wg         sync.WaitGroup
}

func NewJobRunner(job sched.Job, saga saga.Saga, sagaState *saga.SagaState, nodes []cm.Node) jobRunner {
	jr := jobRunner{
		job:        job,
		saga:       saga,
		sagaState:  sagaState,
		dist:       dist.NewPoolDistributor(cm.StaticClusterFactory(nodes)),
		updateChan: make(chan saga.SagaMessageType),
	}

	go jr.updateSagaState()
	return jr
}

func (jr jobRunner) runJob() error {
	// don't re-run an already completed saga
	if sagaState.IsSagaCompleted() {
		return
	}

	// if the saga has not been aborted, just run each task in the saga
	// TODO: this can be made smarter to run the next Best Task instead of just
	// the next one in order etc....
	if !sagaState.IsSagaAborted() {
		for _, task := range job.Tasks {

			if !sagaState.IsTaskCompleted(task.Id) {
				node := localDist.ReserveNode()

				updateChan <- sagaUpdate{
					msgType: saga.StartTask,
					taskId:  task.Id,
				}

				//TODO: spin this off in a go routine
				node.SendMessage(task)

				updateChan <- sagaUpdate{
					msgType: saga.EndTask,
					taskId:  task.Id,
				}

				localDist.ReleaseNode(node)
			}
		}
	}

	if sagaState.IsSagaAborted() {
		// TODO: run all compensating tasks to completion
	}

	updateChan <- sagaUpdate{
		msgType: saga.EndSaga,
	}

	return nil
}

type stateUpdate struct {
	msgType saga.SagaMessageType
	taskId  string
	data    []byte
}

// handles all the sagaState updates
func (jr jobRunner) updateSagaState() {

	for update := range jr.updateChan {
		switch update.msgType {
		case saga.StartTask:

			attempts := 0
			state, err := jr.saga.StartTask(jr.sagaState, update.taskId, update.data)

			// if the attempt to update didn't succeed retry
			for err != nil && retryableErr(err) && attempts < MAX_RETRY {
				state, err = jr.saga.StartTask(jr.sagaState, update.taskId, update.data)
				attempts++
			}

			// if state transition was successful update internal state
			if state != nil {
				jr.sagaState = state

				// state transition was not successful maxed out retries
				// TODO: for now just panic eventually implement dead letter queue.
			} else {
				panic(fmt.Sprintf("Failed to succeesfully log StartTask. sagaId: %v, taskId: %v", jr.sagaState.SagaId(), update.taskId))
			}

		case saga.EndTask:
			attempts := 0
			state, err := jr.saga.EndTask(jr.sagaState, update.taskId, update.data)

			// if the attempt to update didn't succeed retry
			for err != nil && retryableErr(err) && attempts < MAX_RETRY {
				state, err = jr.saga.EndTask(jr.sagaState, update.taskId, update.data)
				attempts++
			}

			// if state transition was successful update internal state
			if state != nil {
				jr.sagaState = state

				// state transition was not successful maxed out retries
				// TODO: for now just panic eventually implement dead letter queue.
			} else {
				panic(fmt.Sprintf("Failed to succeesfully log EndTask sagaId: %v, taskId: %v", jr.sagaState.SagaId(), update.taskId))
			}

		case saga.StartCompTask:
			attempts := 0
			state, err := jr.saga.StartCompTask(jr.sagaState, update.taskId, update.data)

			// if the attempt to update didn't succeed retry
			for err != nil && retryableErr(err) && attempts < MAX_RETRY {
				state, err = jr.saga.StartCompTask(jr.sagaState, update.taskId, update.data)
				attempts++
			}

			// if state transition was successful update internal state
			if state != nil {
				jr.sagaState = state

				// state transition was not successful maxed out retries
				// TODO: for now just panic eventually implement dead letter queue.
			} else {
				panic(fmt.Sprintf("Failed to succeesfully log StartCompTask. sagaId: %v, taskId: %v", jr.sagaState.SagaId(), update.taskId))
			}

		case saga.EndCompTask:
			attempts := 0
			state, err := jr.saga.EndCompTask(jr.sagaState, update.taskId, update.data)

			// if the attempt to update didn't succeed retry
			for err != nil && retryableErr(err) && attempts < MAX_RETRY {
				state, err = jr.saga.EndCompTask(jr.sagaState, update.taskId, update.data)
				attempts++
			}

			// if state transition was successful update internal state
			if state != nil {
				jr.sagaState = state

				// state transition was not successful maxed out retries
				// TODO: for now just panic eventually implement dead letter queue.
			} else {
				panic(fmt.Sprintf("Failed to succeesfully log EndCompTask. sagaId: %v, taskId: %v", jr.sagaState.SagaId(), update.taskId))
			}

		case saga.EndSaga:
			attempts := 0
			state, err := jr.saga.EndSaga(jr.sagaState)

			// if the attempt to update didn't succeed retry
			for err != nil && retryableErr(err) && attempts < MAX_RETRY {
				state, err = jr.saga.EndSaga(jr.sagaState)
				attempts++
			}

			// if state transition was successful update internal state
			if state != nil {
				attempts := 0
				jr.sagaState = state
				// close the channel no more messages should be coming
				close(update)

				// state transition was not successful maxed out retries
				// TODO: for now just panic eventually implement dead letter queue.
			} else {
				panic(fmt.Sprintf("Failed to succeesfully log EndSaga. sagaId: %v", jr.sagaState.SagaId()))
			}

		case saga.AbortSaga:
			attempts := 0
			state, err := jr.saga.AbortSaga(jr.sagaState)

			// if state transition was successful update internal state
			if state != nil {
				jr.sagaState = state
			} else {
				panic(fmt.Sprintf("Failed to successfully log AbortSaga. sagaId: %v", jr.sagaState.SagaId()))
			}
		}
	}
}

// checks the error returned my updating saga state.
func retryableErr(err) bool {

	// InvalidSagaState is an unrecoverable error. This indicates a fatal bug in the code
	// which is asking for an impossible transition.
	invlaidSagaState, ok := err.(saga.InvalidSagaStateError)
	if ok {
		return false
	}

	// InvalidSagaMessage is an unrecoverable error.  This indicates a fatal bug in the code
	// which is applying invalid parameters to a saga.
	invalidSagaMessage, ok2 := err.(saga.InvalidSagaMessageError)
	if ok2 {
		return false
	}

	// InvalidRequestError is an unrecoverable error.  This indicates a fatal bug in the code
	// where the SagaLog cannot durably store messages
	invalidRequestError, ok3 := err.(saga.InvalidRequestError)
	if ok3 {
		return false
	}

	// InternalLogError is a transient error experienced by the log.  It was not
	// able to durably store the message but it is ok to retry.
	internalLogError, ok4 := err.(internalLogError)
	if ok4 {
		return true
	}

	// unknown error, default to retryable.
	return true
}
