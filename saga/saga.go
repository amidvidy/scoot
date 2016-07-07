package saga

import (
	"sync"
)

//
// Saga Object which provides all Saga Functionality
// Implementations of SagaLog should provide a factory method
// which returns a saga based on its implementation.
//
type Saga struct {
	log           SagaLog
	sagaStateMap  map[string]*SagaState
	sagaUpdateMap map[string]chan sagaUpdate
	mutex         *sync.RWMutex
}

//
// Make a Saga which uses the specied SagaLog interface for durable storage
//
func MakeSaga(log SagaLog) Saga {
	return Saga{
		log:           log,
		sagaStateMap:  make(map[string]*SagaState),
		sagaUpdateMap: make(map[string]chan sagaUpdate),
		mutex:         &sync.RWMutex{},
	}
}

//
// Start Saga. Logs Message message to the log.
// Returns the resulting SagaState or an error if it fails.
//
func (s Saga) StartSaga(sagaId string, job []byte) (*SagaState, error) {

	//TODO: Check that Saga doesn't arleady exist

	//Create new SagaState
	state, err := makeSagaState(sagaId, job)
	if err != nil {
		return nil, err
	}

	//Durably Store that we Created a new Saga
	err = s.log.StartSaga(sagaId, job)
	if err != nil {
		return nil, err
	}

	// setup internal saga state for processing updates for this saga
	s.initializeSagaUpdate(sagaId, state)
	return state, nil
}

//
// Log an End Saga Message to the log, returns updated SagaState
// Returns the resulting SagaState or an error if it fails
//
func (s Saga) EndSaga(sagaId string) (*SagaState, error) {
	return s.updateSagaState(sagaId, MakeEndSagaMessage(sagaId))
}

//
// Log an AbortSaga message.  This indicates that the
// Saga has failed and all execution should be stopped
// and compensating transactions should be applied.
//
// Returns the resulting SagaState or an error if it fails
//
func (s Saga) AbortSaga(sagaId string) (*SagaState, error) {
	return s.updateSagaState(sagaId, MakeAbortSagaMessage(sagaId))
}

//
// Log a StartTask Message to the log.  Returns
// an error if it fails.
//
// StartTask is idempotent with respect to sagaId & taskId.  If
// the data passed changes the last written StartTask message will win
//
// Returns the resulting SagaState or an error if it fails
//
func (s Saga) StartTask(sagaId string, taskId string, data []byte) (*SagaState, error) {
	return s.updateSagaState(sagaId, MakeStartTaskMessage(sagaId, taskId, data))
}

//
// Log an EndTask Message to the log.  Indicates that this task
// has been successfully completed. Returns an error if it fails.
//
// EndTask is idempotent with respect to sagaId & taskId.  If
// the data passed changes the last written EndTask message will win
//
// Returns the resulting SagaState or an error if it fails
//
func (s Saga) EndTask(sagaId string, taskId string, results []byte) (*SagaState, error) {
	return s.updateSagaState(sagaId, MakeEndTaskMessage(sagaId, taskId, results))
}

//
// Log a Start Compensating Task Message to the log. Should only be logged after a Saga
// has been avoided and in Rollback Recovery Mode. Should not be used in ForwardRecovery Mode
// returns an error if it fails
//
// StartCompTask is idempotent with respect to sagaId & taskId.  If
// the data passed changes the last written StartCompTask message will win
//
// Returns the resulting SagaState or an error if it fails
//
func (s Saga) StartCompensatingTask(sagaId string, taskId string, data []byte) (*SagaState, error) {
	return s.updateSagaState(sagaId, MakeStartCompTaskMessage(sagaId, taskId, data))
}

//
// Log an End Compensating Task Message to the log when a Compensating Task
// has been successfully completed. Returns an error if it fails.
//
// EndCompTask is idempotent with respect to sagaId & taskId.  If
// the data passed changes the last written EndCompTask message will win
//
// Returns the resulting SagaState or an error if it fails
//
func (s Saga) EndCompensatingTask(sagaId string, taskId string, results []byte) (*SagaState, error) {
	return s.updateSagaState(sagaId, MakeEndCompTaskMessage(sagaId, taskId, results))
}

//
// Should be called at Saga Creation time.
// Returns a Slice of In Progress SagaIds
//
func (s Saga) Startup() ([]string, error) {

	ids, err := s.log.GetActiveSagas()
	if err != nil {
		return nil, err
	}

	return ids, nil
}

//
// Recovers SagaState by reading all logged messages from the log.
// Utilizes the specified recoveryType to determine if Saga needs to be
// Aborted or can proceed safely.
//
// Returns the current SagaState
//
func (s Saga) RecoverSagaState(sagaId string, recoveryType SagaRecoveryType) (*SagaState, error) {
	state, err := recoverState(sagaId, s, recoveryType)

	if err != nil {
		return nil, err
	}

	// now that we've recovered the saga initialize its update path
	s.initializeSagaUpdate(sagaId, state)

	// Check if we can safely proceed forward based on recovery method
	// RollbackRecovery must check if in a SafeState,
	// ForwardRecovery can always make progress
	switch recoveryType {

	case RollbackRecovery:

		// if Saga is not in a safe state we must abort the saga
		// And compensating tasks should start
		if !isSagaInSafeState(state) {
			state, err = s.AbortSaga(sagaId)
			if err != nil {
				return nil, err
			}
		}

	case ForwardRecovery:
		// Nothing to do on Forward Recovery
	}

	return state, err
}

//
// logs the specified message durably to the SagaLog & updates internal state if its a valid state transition
//
func (s Saga) logMessage(state *SagaState, msg sagaMessage) (*SagaState, error) {

	//verify that the applied message results in a valid state
	newState, err := updateSagaState(state, msg)
	if err != nil {
		return nil, err
	}

	//try durably storing the message
	err = s.log.LogMessage(msg)
	if err != nil {
		return nil, err
	}

	return newState, nil
}

// initializes datastructures need for sagaUpdate path,
// spins off a go routine to process updates as they come in
func (s Saga) initializeSagaUpdate(sagaId string, state *SagaState) {
	updateCh := make(chan sagaUpdate, 0)

	s.mutex.Lock()
	s.sagaUpdateMap[sagaId] = updateCh
	s.sagaStateMap[sagaId] = state
	s.mutex.Unlock()

	go s.updateSagaStateLoop(sagaId, updateCh)
}

type sagaUpdateResult struct {
	state *SagaState
	err   error
}

type sagaUpdate struct {
	state    *SagaState
	msg      sagaMessage
	resultCh chan sagaUpdateResult
}

// updateSagaStateLoop that is executed inside of a single go routine.  There
// is one per saga currently executing.  This ensures all updates are applied
// in order to a saga.  Also controls access to the SagaState so its is
// updated in a thread safe manner
func (s Saga) updateSagaStateLoop(sagaId string, updateCh chan sagaUpdate) {

	for update := range updateCh {

		s.mutex.RLock()
		currState, _ := s.sagaStateMap[sagaId]
		s.mutex.RUnlock()

		newState, err := s.logMessage(currState, update.msg)

		if err == nil {

			s.mutex.Lock()
			s.sagaStateMap[sagaId] = newState
			s.mutex.Unlock()

			update.resultCh <- sagaUpdateResult{
				state: newState,
				err:   nil,
			}

		} else {
			update.resultCh <- sagaUpdateResult{
				state: nil,
				err:   err,
			}
		}
	}
}

// adds a message for updateSagaStateLoop to execute to the channel for the
// specified saga.  blocks until the message has been applied
func (s Saga) updateSagaState(sagaId string, msg sagaMessage) (*SagaState, error) {

	s.mutex.RLock()
	currState, _ := s.sagaStateMap[sagaId]
	updateCh, _ := s.sagaUpdateMap[sagaId]
	s.mutex.RUnlock()

	//TODO: check not ok means saga doesn't exist

	resultCh := make(chan sagaUpdateResult, 0)
	updateCh <- sagaUpdate{
		state:    currState,
		msg:      msg,
		resultCh: resultCh,
	}

	result := <-resultCh
	return result.state, result.err
}
