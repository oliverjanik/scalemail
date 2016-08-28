package emailq

import (
	"bytes"
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

	q = queue
	r := m.Run()

	q.Close()
	err = os.Remove(testDb)
	if err != nil {
		panic(err)
	}

	os.Exit(r)
}

func TestNormalFlow(t *testing.T) {
	err := q.Push(createMsg())
	if err != nil {
		t.Fatal("Error pushing:", err)
	}

	key, _, err := q.Pop()
	if err != nil || key == nil {
		t.Fatal("Error popping:", err)
	}

	err = q.RemoveDelivered(key)
	if err != nil {
		t.Fatal("Error removing delivered:", err)
	}
}

func TestRetryFlow(t *testing.T) {
	err := q.Push(createMsg())

	key, _, err := q.Pop()
	if err != nil || key == nil {
		t.Fatal("Error popping:", err)
	}

	err = q.Retry(key)
	if err != nil {
		t.Fatal("Error pushing retry:", err)
	}

	key, _, err = q.Pop()
	if key != nil {
		t.Fatal("Retry needs to wait")
	}
}

func TestDeadFlow(t *testing.T) {
	err := q.Push(createMsg())

	key, _, err := q.Pop()
	if err != nil || key == nil {
		t.Fatal("Error popping:", err)
	}

	err = q.Kill(key)
	if err != nil {
		t.Fatal("Error pushing dead letter:", err)
	}
}

func TestCrashFlow(t *testing.T) {
	err := q.Push(createMsg())

	k1, msg1, err := q.Pop()
	if err != nil || k1 == nil {
		t.Fatal("Error popping:", err)
	}

	err = q.Recover()
	if err != nil {
		t.Fatal("Error recovering:", err)
	}

	k2, msg2, err := q.Pop()
	if err != nil {
		t.Fatal("Error popping:", err)
	}

	if bytes.Equal(k1, k2) {
		t.Fatal("Message should get a new key", string(k1), string(k2))
	}

	if msg1.From != msg2.From {
		t.Fatal("Outgoing message does not match", string(k1), string(k2))
	}

	err = q.RemoveDelivered(k2)
	if err != nil {
		t.Fatal("Error removing delivered:", err)
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
