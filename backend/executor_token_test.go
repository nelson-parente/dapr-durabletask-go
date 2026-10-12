package backend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/dapr/durabletask-go/api/protos"
)

func taskScheduledEvent(taskExecutionID string, stamp *timestamppb.Timestamp) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   3,
		Timestamp: stamp,
		EventType: &protos.HistoryEvent_TaskScheduled{TaskScheduled: &protos.TaskScheduledEvent{
			Name:            "act",
			TaskExecutionId: taskExecutionID,
		}},
	}
}

func Test_activityCompletionToken(t *testing.T) {
	stamp := timestamppb.New(time.Unix(1700000000, 123))

	tests := []struct {
		name  string
		event *protos.HistoryEvent
		want  string
	}{
		{name: "TaskExecutionId and timestamp", event: taskScheduledEvent("exec-1", stamp), want: "exec-1@1700000000.123"},
		{name: "TaskExecutionId only", event: taskScheduledEvent("exec-1", nil), want: "exec-1"},
		{name: "timestamp only (SDK without TaskExecutionId)", event: taskScheduledEvent("", stamp), want: "@1700000000.123"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, activityCompletionToken(tt.event))
			assert.Equal(t, tt.want, activityCompletionToken(tt.event), "the token is a function of the scheduling, so a re-dispatch gets the same token")
		})
	}

	t.Run("no scheduling identity: a fresh token per dispatch", func(t *testing.T) {
		event := taskScheduledEvent("", nil)
		first, second := activityCompletionToken(event), activityCompletionToken(event)
		require.Len(t, first, 36)
		assert.NotEqual(t, first, second)
	})
}
