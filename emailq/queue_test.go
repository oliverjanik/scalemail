package emailq

import (
	"os"
	"testing"
)

const (
	testDb = "test.db"
)

var (
	q *EmailQ
)

func TestMain(m *testing.M) {
	queue, err := New(testDb)
	if err != nil {
		panic(err)
	}
	defer func() {
		queue.Close()
		os.Remove(testDb)
	}()

	q = queue
	os.Exit(m.Run())
}

func TestNormalFlow(t *testing.T) {
	err := q.Push(createMsg())
	if err != nil {
		t.Error("Error pushing:", err)
	}

	key, _, err := q.PopIncoming()
	if err != nil {
		t.Error("Error popping:", err)
	}

	err = q.RemoveDelivered(key)
	if err != nil {
		t.Error("Error removing delivered:", err)
	}
}

func TestRetryFlow(t *testing.T) {
	err := q.Push(createMsg())

	key, _, err := q.PopIncoming()
	if err != nil {
		t.Error("Error popping:", err)
	}

	err = q.PushRetry(key)
	if err != nil {
		t.Error("Error pushing retry:", err)
	}

	key, _, err = q.PopRetry()
	if err != nil {
		t.Error("Error popping retry:", err)
	}

	err = q.RemoveDelivered(key)
	if err != nil {
		t.Error("Error removing delivered:", err)
	}
}

func TestDeadFlow(t *testing.T) {
	err := q.Push(createMsg())

	key, _, err := q.PopIncoming()
	if err != nil {
		t.Error("Error popping:", err)
	}

	err = q.PushRetry(key)
	if err != nil {
		t.Error("Error pushing retry:", err)
	}

	key, _, err = q.PopRetry()
	if err != nil {
		t.Error("Error popping retry:", err)
	}

	err = q.PushDeadLetter(key)
	if err != nil {
		t.Error("Error pushing dead letter:", err)
	}
}

func createMsg() *Msg {
	return &Msg{
		Host: "host",
		From: "from",
		To:   []string{"a", "b"},
		Data: nil,
	}
}
