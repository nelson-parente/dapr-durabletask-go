package local_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/local"
)

// host models one sidecar process: its own tasks backend, executor and work
// item stream. Two hosts model a sidecar that died and the one that took
// its work over.
type host struct {
	be     *tasksOnlyBackend
	exec   backend.Executor
	stream *workItemsStream
}

func newHost(t *testing.T) *host {
	t.Helper()
	be := &tasksOnlyBackend{tasks: local.NewTasksBackend()}
	exec, _ := backend.NewGrpcExecutor(be, backend.DefaultLogger())
	server := exec.(protos.TaskHubSidecarServiceServer)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	stream := &workItemsStream{ctx: ctx, items: make(chan *protos.WorkItem, 4)}
	go func() { _ = server.GetWorkItems(&protos.GetWorkItemsRequest{}, stream) }()
	return &host{be: be, exec: exec, stream: stream}
}

// dispatch runs ExecuteActivity for event on the host and returns the token
// of the work item sent to the worker and the channel its result lands on.
func (h *host) dispatch(t *testing.T, ctx context.Context, event *protos.HistoryEvent) (token string, results <-chan string) {
	t.Helper()
	out := make(chan string, 1)
	go func() {
		ev, err := h.exec.ExecuteActivity(ctx, api.InstanceID("wf1"), event, backend.ExecuteOptions{})
		if err != nil {
			out <- "error: " + err.Error()
			return
		}
		out <- ev.GetTaskCompleted().GetResult().GetValue()
	}()
	select {
	case wi := <-h.stream.items:
		return wi.GetCompletionToken(), out
	case <-time.After(5 * time.Second):
		require.FailNow(t, "work item was not dispatched")
		return "", out
	}
}

func scheduled(taskExecutionID string, stamp *timestamppb.Timestamp) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   7,
		Timestamp: stamp,
		EventType: &protos.HistoryEvent_TaskScheduled{TaskScheduled: &protos.TaskScheduledEvent{
			Name:            "ship",
			TaskExecutionId: taskExecutionID,
		}},
	}
}

func response(token, result string) *protos.ActivityResponse {
	return &protos.ActivityResponse{
		InstanceId:      "wf1",
		TaskId:          7,
		Result:          wrapperspb.String(result),
		CompletionToken: token,
	}
}

// A sidecar dies while the worker runs an activity. The next sidecar
// re-dispatches the same TaskScheduled event when the worker reconnects, and
// the worker then completes its first execution with the first sidecar's
// token. The re-dispatch must settle on that response: it is the result of
// the same scheduling. Waiting for the second execution instead costs one
// activity duration on every such recovery.
func Test_redispatchOfSameSchedulingAcceptsEarlierAttemptsResponse(t *testing.T) {
	event := scheduled("exec-1", timestamppb.New(time.Unix(1700000000, 123)))

	hostA := newHost(t)
	ctxA, cancelA := context.WithCancel(t.Context())
	tokenA, _ := hostA.dispatch(t, ctxA, event)
	cancelA()

	hostB := newHost(t)
	tokenB, results := hostB.dispatch(t, t.Context(), event)

	require.NoError(t, hostB.be.CompleteActivityTask(t.Context(), response(tokenA, "first execution")))
	select {
	case got := <-results:
		assert.Equal(t, "first execution", got)
	case <-time.After(2 * time.Second):
		require.FailNow(t, "the re-dispatch did not settle on the earlier attempt's response; it waits for a second execution of the activity")
	}
	assert.Equal(t, tokenA, tokenB, "every dispatch of one scheduling carries the same token")
}

// A response for another scheduling that re-uses the task ID (an instance
// recreated under the same ID, or a continued-as-new generation) is still
// discarded, and the execution settles on its own response. This is the
// protection #121 added.
func Test_redispatchDiscardsResponseOfAnotherScheduling(t *testing.T) {
	stamp1 := timestamppb.New(time.Unix(1700000000, 123))
	stamp2 := timestamppb.New(time.Unix(1700000000, 124))
	tests := []struct {
		name     string
		previous *protos.HistoryEvent
		current  *protos.HistoryEvent
	}{
		{name: "other TaskExecutionId", previous: scheduled("exec-1", stamp1), current: scheduled("exec-2", stamp1)},
		{name: "other timestamp, SDK without TaskExecutionId", previous: scheduled("", stamp1), current: scheduled("", stamp2)},
		{name: "other TaskExecutionId, no timestamp", previous: scheduled("exec-1", nil), current: scheduled("exec-2", nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hostA := newHost(t)
			ctxA, cancelA := context.WithCancel(t.Context())
			tokenA, _ := hostA.dispatch(t, ctxA, tt.previous)
			cancelA()

			hostB := newHost(t)
			tokenB, results := hostB.dispatch(t, t.Context(), tt.current)
			require.NotEqual(t, tokenA, tokenB)

			require.NoError(t, hostB.be.CompleteActivityTask(t.Context(), response(tokenA, "previous scheduling")))
			select {
			case got := <-results:
				require.FailNow(t, "settled on another scheduling's response", "got %q", got)
			case <-time.After(300 * time.Millisecond):
			}

			require.NoError(t, hostB.be.CompleteActivityTask(t.Context(), response(tokenB, "current scheduling")))
			select {
			case got := <-results:
				assert.Equal(t, "current scheduling", got)
			case <-time.After(2 * time.Second):
				require.FailNow(t, "the execution did not settle on its own response")
			}
		})
	}
}

// A TaskScheduled event with neither a TaskExecutionId nor a timestamp has
// no scheduling identity, so each dispatch keeps a token of its own, as
// before.
func Test_dispatchWithoutSchedulingIdentityKeepsPerDispatchToken(t *testing.T) {
	event := scheduled("", nil)

	hostA := newHost(t)
	ctxA, cancelA := context.WithCancel(t.Context())
	tokenA, _ := hostA.dispatch(t, ctxA, event)
	cancelA()

	hostB := newHost(t)
	tokenB, _ := hostB.dispatch(t, t.Context(), event)
	require.NotEqual(t, tokenA, tokenB)
}

// Two executions of one scheduling can be pending on the same host, for
// example a re-dispatch while the first dispatch is still waiting. They
// carry one token, so one response settles both. Each yields a completion
// event for the same task, and the runtime state drops the second as a
// duplicate.
func Test_concurrentDispatchesOfSameSchedulingSettleOnOneResponse(t *testing.T) {
	event := scheduled("exec-1", timestamppb.New(time.Unix(1700000000, 123)))

	h := newHost(t)
	token1, results1 := h.dispatch(t, t.Context(), event)
	token2, results2 := h.dispatch(t, t.Context(), event)
	require.Equal(t, token1, token2)

	require.NoError(t, h.be.CompleteActivityTask(t.Context(), response(token1, "result")))
	for _, results := range []<-chan string{results1, results2} {
		select {
		case got := <-results:
			assert.Equal(t, "result", got)
		case <-time.After(2 * time.Second):
			require.FailNow(t, "an execution of the scheduling did not settle on the response")
		}
	}
}
