package saga

import (
	"errors"
	"fmt"
	"github.com/golang/mock/gomock"
	"testing"
)

func TestStartSaga(t *testing.T) {

	id := "testSaga"
	var job []byte

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga(id, job)

	s := MakeSaga(sagaLogMock)
	state, err := s.StartSaga(id, job)

	if err != nil {
		t.Error("Expected StartSaga to not return an error")
	}

	if state.SagaId() != id {
		t.Error("Expected state.SagaId to equal 'testSaga'")
	}
}

func TestStartSagaLogError(t *testing.T) {
	id := "testSaga"
	var job []byte

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga(id, job).Return(errors.New("Failed to Log StartSaga"))

	s := MakeSaga(sagaLogMock)
	state, err := s.StartSaga(id, job)

	if err == nil {
		t.Error("Expected StartSaga to return error if SagaLog fails to log request")
	}
	if state != nil {
		t.Error("Expected returned state to be nil when error occurs")
	}
}

func TestEndSaga(t *testing.T) {
	entry := MakeEndSagaMessage("testSaga")

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(entry)

	s := MakeSaga(sagaLogMock)
	state, err := s.StartSaga("testSaga", nil)

	state, err = s.EndSaga("testSaga")
	if err != nil {
		t.Error("Expected EndSaga to not return an error")
	}

	if !state.IsSagaCompleted() {
		t.Error("Expected Saga to be completed")
	}
}

func TestEndSagaLogError(t *testing.T) {
	entry := MakeEndSagaMessage("testSaga")

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log EndSaga Message"))

	s := MakeSaga(sagaLogMock)
	_, err := s.StartSaga("testSaga", nil)

	state, err := s.EndSaga("testSaga")
	if err == nil {
		t.Error("Expected EndSaga to not return an error when write to SagaLog Fails")
	}

	if state != nil {
		t.Error("Expected state to be nil when error returned")
	}
}

func TestAbortSaga(t *testing.T) {
	entry := MakeAbortSagaMessage("testSaga")

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(entry)

	s := MakeSaga(sagaLogMock)
	_, err := s.StartSaga("testSaga", nil)

	state, err := s.AbortSaga("testSaga")
	if err != nil {
		t.Error("Expected AbortSaga to not return an error")
	}

	if !state.IsSagaAborted() {
		t.Error("expected Saga to be Aborted")
	}
}

func TestAbortSagaLogError(t *testing.T) {
	entry := MakeAbortSagaMessage("testSaga")

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log AbortSaga Message"))

	s := MakeSaga(sagaLogMock)

	state, err := s.StartSaga("testSaga", nil)

	state, err = s.AbortSaga("testSaga")
	if err == nil {
		t.Error("Expected AbortSaga to not return an error when write to SagaLog Fails")
	}

	if state != nil {
		t.Error("Expected returned state to be nil")
	}
}

func TestStartTask(t *testing.T) {
	entry := MakeStartTaskMessage("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(entry)

	s := MakeSaga(sagaLogMock)
	state, err := s.StartSaga("testSaga", nil)

	state, err = s.StartTask("testSaga", "task1", nil)
	if err != nil {
		t.Error("Expected StartTask to not return an error")
	}

	if !state.IsTaskStarted("task1") {
		t.Error("expected task1 to be started")
	}
}

func TestStartTaskLogError(t *testing.T) {
	entry := MakeStartTaskMessage("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log StartTask Message"))

	s := MakeSaga(sagaLogMock)

	state, err := s.StartSaga("testSaga", nil)

	state, err = s.StartTask("testSaga", "task1", nil)
	if err == nil {
		t.Error("Expected StartTask to not return an error when write to SagaLog Fails")
	}

	if state != nil {
		t.Error("Expected state to be nil")
	}
}

func TestEndTask(t *testing.T) {
	entry := MakeEndTaskMessage("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(MakeStartTaskMessage("testSaga", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(entry)

	s := MakeSaga(sagaLogMock)

	state, err := s.StartSaga("testSaga", nil)
	state, err = s.StartTask("testSaga", "task1", nil)

	state, err = s.EndTask("testSaga", "task1", nil)
	if err != nil {
		t.Error("Expected EndTask to not return an error")
	}

	if !state.IsTaskCompleted("task1") {
		t.Error("expected task1 to be completed")
	}
}

func TestEndTaskLogError(t *testing.T) {
	entry := MakeEndTaskMessage("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(MakeStartTaskMessage("testSaga", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log EndTask Message"))

	s := MakeSaga(sagaLogMock)

	state, err := s.StartSaga("testSaga", nil)
	state, err = s.StartTask("testSaga", "task1", nil)

	state, err = s.EndTask("testSaga", "task1", nil)
	if err == nil {
		t.Error("Expected EndTask to not return an error when write to SagaLog Fails")
	}

	if state != nil {
		t.Error("Expected State to be Nil")
	}
}

func TestStartCompTask(t *testing.T) {
	entry := MakeStartCompTaskMessage("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(MakeStartTaskMessage("testSaga", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(MakeAbortSagaMessage("testSaga"))
	sagaLogMock.EXPECT().LogMessage(entry)

	s := MakeSaga(sagaLogMock)

	state, err := s.StartSaga("testSaga", nil)
	state, err = s.StartTask("testSaga", "task1", nil)
	state, err = s.AbortSaga("testSaga")

	state, err = s.StartCompensatingTask("testSaga", "task1", nil)
	if err != nil {
		t.Error(fmt.Sprintf("Expected StartCompensatingTask to not return an error: %s", err))
	}

	if !state.IsCompTaskStarted("task1") {
		t.Error("Expected Comp Task to be started")
	}
}

func TestStartCompTaskLogError(t *testing.T) {
	entry := MakeStartCompTaskMessage("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(MakeStartTaskMessage("testSaga", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(MakeAbortSagaMessage("testSaga"))
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log StartCompTask Message"))

	s := MakeSaga(sagaLogMock)

	state, err := s.StartSaga("testSaga", nil)
	state, err = s.StartTask("testSaga", "task1", nil)
	state, err = s.AbortSaga("testSaga")

	state, err = s.StartCompensatingTask("testSaga", "task1", nil)
	if err == nil {
		t.Error("Expected StartCompTask to not return an error when write to SagaLog Fails")
	}

	if state != nil {
		t.Error("Expected returned state to be nil")
	}
}

func TestEndCompTask(t *testing.T) {
	entry := MakeEndCompTaskMessage("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(MakeStartTaskMessage("testSaga", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(MakeAbortSagaMessage("testSaga"))
	sagaLogMock.EXPECT().LogMessage(MakeStartCompTaskMessage("testSaga", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(entry)

	s := MakeSaga(sagaLogMock)

	state, err := s.StartSaga("testSaga", nil)
	state, err = s.StartTask("testSaga", "task1", nil)
	state, err = s.AbortSaga("testSaga")
	state, err = s.StartCompensatingTask("testSaga", "task1", nil)

	state, err = s.EndCompensatingTask("testSaga", "task1", nil)
	if err != nil {
		t.Error("Expected EndCompensatingTask to not return an error")
	}

	if !state.IsCompTaskCompleted("task1") {
		t.Error("Expected Comp task1 to be completed")
	}
}

func TestEndCompTaskLogError(t *testing.T) {
	entry := MakeEndCompTaskMessage("testSaga", "task1", nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().StartSaga("testSaga", nil)
	sagaLogMock.EXPECT().LogMessage(MakeStartTaskMessage("testSaga", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(MakeAbortSagaMessage("testSaga"))
	sagaLogMock.EXPECT().LogMessage(MakeStartCompTaskMessage("testSaga", "task1", nil))
	sagaLogMock.EXPECT().LogMessage(entry).Return(errors.New("Failed to Log EndCompTask Message"))

	s := MakeSaga(sagaLogMock)

	state, err := s.StartSaga("testSaga", nil)
	state, err = s.StartTask("testSaga", "task1", nil)
	state, err = s.AbortSaga("testSaga")
	state, err = s.StartCompensatingTask("testSaga", "task1", nil)

	state, err = s.EndCompensatingTask("testSaga", "task1", nil)
	if err == nil {
		t.Error("Expected EndCompTask to not return an error when write to SagaLog Fails")
	}

	if state != nil {
		t.Error("Expected state to be nil")
	}
}

func TestStartup_ReturnsError(t *testing.T) {

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetActiveSagas().Return(nil, errors.New("test error"))

	s := MakeSaga(sagaLogMock)
	ids, err := s.Startup()

	if err == nil {
		t.Error("Expected error to not be nil")
	}
	if ids != nil {
		t.Error("ids should be null when error is returned")
	}
}

func TestStartup_ReturnsIds(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetActiveSagas().Return([]string{"saga1", "saga2", "saga3"}, nil)

	s := MakeSaga(sagaLogMock)
	ids, err := s.Startup()

	if err != nil {
		t.Error(fmt.Sprintf("unexpected error returned %s", err))
	}
	if ids == nil {
		t.Error("expected is to be returned")
	}

	expectedIds := make(map[string]bool)
	expectedIds["saga1"] = true
	expectedIds["saga2"] = true
	expectedIds["saga3"] = true

	for _, id := range ids {
		if !expectedIds[id] {
			t.Error(fmt.Sprintf("unexpectedId returend %s", id))
		}
	}
}

func TestRecoverSagaState(t *testing.T) {

	sagaId := "saga1"
	taskId := "task1"

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	msgs := []sagaMessage{
		MakeStartSagaMessage(sagaId, nil),
		MakeStartTaskMessage(sagaId, taskId, nil),
		MakeEndTaskMessage(sagaId, taskId, nil),
		MakeEndSagaMessage(sagaId),
	}

	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetMessages(sagaId).Return(msgs, nil)

	s := MakeSaga(sagaLogMock)
	state, err := s.RecoverSagaState(sagaId, ForwardRecovery)

	if err != nil {
		t.Error(fmt.Sprintf("unexpected error returned %s", err))
	}
	if state == nil {
		t.Error("expected returned state to not be nil")
	}

	if !state.IsSagaCompleted() {
		t.Error("expected returned saga state to be completed saga")
	}
}

func TestRecoverSagaState_ReturnsError(t *testing.T) {
	sagaId := "saga1"

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	sagaLogMock := NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetMessages(sagaId).Return(nil, errors.New("test error"))

	s := MakeSaga(sagaLogMock)
	state, err := s.RecoverSagaState(sagaId, RollbackRecovery)

	if err == nil {
		t.Error("expeceted error to not be nil")
	}

	if state != nil {
		t.Error("expected returned state to be nil when error occurs")
	}
}
