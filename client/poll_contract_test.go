package client_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/niqbal/workflow-engine/client"
	"github.com/niqbal/workflow-engine/internal/httpapi"
	"github.com/niqbal/workflow-engine/internal/server/api"
)

// blockingPollService long-polls like the real server: it reports when a poll arrives, then
// waits until the request context is done and returns (nil, nil), as api.Service requires.
type blockingPollService struct {
	okService
	arrived chan struct{}
}

func (s *blockingPollService) PollWorkflowTask(ctx context.Context, _ api.PollRequest) (*api.WorkflowTask, error) {
	s.arrived <- struct{}{}
	<-ctx.Done()
	return nil, nil
}

func (s *blockingPollService) PollActivityTask(ctx context.Context, _ api.PollRequest) (*api.ActivityTask, error) {
	s.arrived <- struct{}{}
	<-ctx.Done()
	return nil, nil
}

// The client claims to implement api.Service so workers can use it in place of an in-process
// server. That contract says a poll returns (nil, nil) when no task arrives before ctx is done.
func TestPollReturnsNilNilWhenContextDoneMidPoll(t *testing.T) {
	polls := map[string]func(ctx context.Context, c *client.Client) (bool, error){
		"workflow": func(ctx context.Context, c *client.Client) (bool, error) {
			task, err := c.PollWorkflowTask(ctx, api.PollRequest{TaskQueue: "q"})
			return task != nil, err
		},
		"activity": func(ctx context.Context, c *client.Client) (bool, error) {
			task, err := c.PollActivityTask(ctx, api.PollRequest{TaskQueue: "q"})
			return task != nil, err
		},
	}
	for _, name := range []string{"workflow", "activity"} {
		t.Run(name, func(t *testing.T) {
			svc := &blockingPollService{arrived: make(chan struct{}, 1)}
			srv := httptest.NewServer(httpapi.New(svc, httpapi.Options{}))
			t.Cleanup(srv.Close)
			c := client.New(srv.URL, client.Options{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				<-svc.arrived
				cancel()
			}()
			got, err := polls[name](ctx, c)
			if got || err != nil {
				t.Fatalf("poll ended by ctx: task=%v err=%v; want nil task and nil error (api.Service contract)", got, err)
			}
		})
	}
}

func TestPollReturnsNilNilWithAlreadyDoneContext(t *testing.T) {
	srv := httptest.NewServer(httpapi.New(okService{}, httpapi.Options{}))
	t.Cleanup(srv.Close)
	c := client.New(srv.URL, client.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if task, err := c.PollWorkflowTask(ctx, api.PollRequest{TaskQueue: "q"}); task != nil || err != nil {
		t.Fatalf("workflow poll with done ctx: %v, %v; want nil, nil", task, err)
	}
	if task, err := c.PollActivityTask(ctx, api.PollRequest{TaskQueue: "q"}); task != nil || err != nil {
		t.Fatalf("activity poll with done ctx: %v, %v; want nil, nil", task, err)
	}
}
