package server

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/niqbal/workflow-engine/internal/server/api"
)

func flipSignedTokenByte(t *testing.T, tok string) string {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		t.Fatalf("decode token %q: %v", tok, err)
	}
	if len(raw) == 0 {
		t.Fatal("token has no bytes")
	}
	raw[len(raw)-1] ^= 1
	return base64.RawURLEncoding.EncodeToString(raw)
}

func TestTesterWorkflowTokenRejectsWrongKeyAndFlippedByte(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	if _, err := s.StartWorkflow(ctx, startReq("wf", "r")); err != nil {
		t.Fatal(err)
	}
	wt := pollNow(t, s)

	decoded, err := s.decodeWorkflowTaskToken(wt.TaskToken)
	if err != nil {
		t.Fatalf("decode real token: %v", err)
	}

	wrongKey := append([]byte(nil), s.cfg.TokenKey...)
	wrongKey[0] ^= 1
	wrongKeySigned, err := encodeWorkflowTaskToken(wrongKey, decoded)
	if err != nil {
		t.Fatalf("encode wrong-key token: %v", err)
	}
	flipped := flipSignedTokenByte(t, wt.TaskToken)

	for _, bad := range []string{wrongKeySigned, flipped} {
		if err := s.CompleteWorkflowTask(ctx, api.CompleteWorkflowTaskRequest{TaskToken: bad}); !errors.Is(err, api.ErrInvalidArgument) {
			t.Fatalf("complete with %q: %v", bad, err)
		}
		if err := s.FailWorkflowTask(ctx, api.FailWorkflowTaskRequest{TaskToken: bad}); !errors.Is(err, api.ErrInvalidArgument) {
			t.Fatalf("fail with %q: %v", bad, err)
		}
	}
}

func TestTesterActivityTokenRejectsWrongKeyAndFlippedByte(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()
	scheduleTestActivity(t, ctx, s)
	at := pollActNow(t, s)

	decoded, err := s.decodeActivityTaskToken(at.TaskToken)
	if err != nil {
		t.Fatalf("decode real token: %v", err)
	}

	wrongKey := append([]byte(nil), s.cfg.TokenKey...)
	wrongKey[0] ^= 1
	wrongKeySigned, err := encodeActivityTaskToken(wrongKey, decoded)
	if err != nil {
		t.Fatalf("encode wrong-key token: %v", err)
	}
	flipped := flipSignedTokenByte(t, at.TaskToken)

	for _, bad := range []string{wrongKeySigned, flipped} {
		if err := s.CompleteActivityTask(ctx, api.CompleteActivityTaskRequest{TaskToken: bad}); !errors.Is(err, api.ErrInvalidArgument) {
			t.Fatalf("complete with %q: %v", bad, err)
		}
		if err := s.FailActivityTask(ctx, api.FailActivityTaskRequest{TaskToken: bad}); !errors.Is(err, api.ErrInvalidArgument) {
			t.Fatalf("fail with %q: %v", bad, err)
		}
	}
}

func TestTesterTaskTokenHelpersRoundTripByType(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	wtIn := workflowTaskToken{Shard: 1, WorkflowID: "wf", RunID: "run", Seq: 7}
	atIn := activityTaskToken{Shard: 1, WorkflowID: "wf", RunID: "run", ScheduledEventID: 11, Attempt: 2}

	wtTok, err := encodeWorkflowTaskToken(key, wtIn)
	if err != nil {
		t.Fatalf("encode workflow token: %v", err)
	}
	atTok, err := encodeActivityTaskToken(key, atIn)
	if err != nil {
		t.Fatalf("encode activity token: %v", err)
	}

	wtOut, err := decodeWorkflowTaskToken(key, wtTok)
	if err != nil {
		t.Fatalf("decode workflow token: %v", err)
	}
	atOut, err := decodeActivityTaskToken(key, atTok)
	if err != nil {
		t.Fatalf("decode activity token: %v", err)
	}

	if wtOut != wtIn {
		t.Fatalf("workflow token round trip mismatch: got %+v want %+v", wtOut, wtIn)
	}
	if atOut != atIn {
		t.Fatalf("activity token round trip mismatch: got %+v want %+v", atOut, atIn)
	}
}
