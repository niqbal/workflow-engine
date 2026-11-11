package client_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/client"
	"github.com/niqbal/workflow-engine/internal/httpapi"
	"github.com/niqbal/workflow-engine/internal/server/api"
)

// rejectPollService rejects every poll with invalid_argument.
type rejectPollService struct{ okService }

func (rejectPollService) PollWorkflowTask(context.Context, api.PollRequest) (*api.WorkflowTask, error) {
	return nil, api.ErrInvalidArgument
}

func (rejectPollService) PollActivityTask(context.Context, api.PollRequest) (*api.ActivityTask, error) {
	return nil, api.ErrInvalidArgument
}

// A ctx whose deadline has passed ends the poll like a cancelled one: no task, no error.
func TestPollPastDeadlineReturnsNilNil(t *testing.T) {
	srv := httptest.NewServer(httpapi.New(okService{}, httpapi.Options{}))
	t.Cleanup(srv.Close)
	c := client.New(srv.URL, client.Options{})
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancel()
	if task, err := c.PollWorkflowTask(ctx, api.PollRequest{TaskQueue: "q"}); task != nil || err != nil {
		t.Fatalf("workflow poll past deadline: %v, %v; want nil, nil", task, err)
	}
	if task, err := c.PollActivityTask(ctx, api.PollRequest{TaskQueue: "q"}); task != nil || err != nil {
		t.Fatalf("activity poll past deadline: %v, %v; want nil, nil", task, err)
	}
}

// Only a poll cut short by ctx becomes (nil, nil); an error the server sends is still reported.
func TestPollErrorReplyStillReported(t *testing.T) {
	srv := httptest.NewServer(httpapi.New(rejectPollService{}, httpapi.Options{}))
	t.Cleanup(srv.Close)
	c := client.New(srv.URL, client.Options{})
	ctx := context.Background()
	if task, err := c.PollWorkflowTask(ctx, api.PollRequest{TaskQueue: "q"}); task != nil || !errors.Is(err, api.ErrInvalidArgument) {
		t.Fatalf("workflow poll: %v, %v; want nil, invalid_argument", task, err)
	}
	if task, err := c.PollActivityTask(ctx, api.PollRequest{TaskQueue: "q"}); task != nil || !errors.Is(err, api.ErrInvalidArgument) {
		t.Fatalf("activity poll: %v, %v; want nil, invalid_argument", task, err)
	}
}
