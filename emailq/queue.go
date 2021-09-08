package emailq

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

var (
	incomingBucket = []byte("incoming")
	outgoingBucket = []byte("outgoing")
	deadBucket     = []byte("deadletter")
)

// EmailQ is a persistent queue that holds the mail messages
type EmailQ struct {
	db *bolt.DB
}

// Msg represents email message
type Msg struct {
	Host  string
	From  string
	To    []string
	Data  []byte
	Retry int
}

// New creates new instance of EmailQ
func New(filepath string) (*EmailQ, error) {
	db, err := bolt.Open(filepath, 0600, nil)
	if err != nil {
		return nil, err
	}

	// create buckets
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(incomingBucket)
		if err != nil {
			return err
		}

		_, err = tx.CreateBucketIfNotExists(outgoingBucket)
		if err != nil {
			return err
		}

		_, err = tx.CreateBucketIfNotExists(deadBucket)
		return err
	})

	if err != nil {
		return nil, err
	}

	return &EmailQ{
		db: db,
	}, nil
}

// Close closes the queue
func (q *EmailQ) Close() error {
	return q.db.Close()
}

// Length returns Incoming queue length
func (q *EmailQ) Length() (count int) {
	q.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(incomingBucket)
		count = b.Stats().KeyN
		return nil
	})

	return
}

// Push messages to the queue
func (q *EmailQ) Push(msg *Msg) error {
	key := []byte(time.Now().UTC().Format(time.RFC3339Nano))
	value := encode(msg)

	err := q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(incomingBucket)
		return b.Put(key, value)
	})

	return err
}

// Retry takes msg from outgoing queue and places that in the Retry queue
func (q *EmailQ) Retry(key []byte) error {
	return q.db.Update(func(tx *bolt.Tx) error {
		outgoing := tx.Bucket(outgoingBucket)

		msg := outgoing.Get(key)
		if msg == nil {
			return fmt.Errorf("Message not found in outgoing bucket")
		}

		err := outgoing.Delete(key)
		if err != nil {
			return err
		}

		incoming := tx.Bucket(incomingBucket)

		m := decode(msg)
		m.Retry++

		t := time.Now().Add(time.Duration(m.Retry*m.Retry*2) * time.Minute)
		key = []byte(t.Format(time.RFC3339Nano))

		msg = encode(m)

		return incoming.Put(key, msg)
	})
}

// Kill takes msg out of outgoing and pushed that to Dead Letter queue
func (q *EmailQ) Kill(key []byte) error {
	return q.db.Update(func(tx *bolt.Tx) error {
		outgoing := tx.Bucket(outgoingBucket)

		msg := outgoing.Get(key)
		if msg == nil {
			return fmt.Errorf("Message not found in outgoing bucket")
		}

		err := outgoing.Delete(key)
		if err != nil {
			return err
		}

		retry := tx.Bucket(deadBucket)

		return retry.Put(key, msg)
	})
}

// Pop get next email from the queue
func (q *EmailQ) Pop() (key []byte, msg *Msg, err error) {
	err = q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(incomingBucket)

		k, v := b.Cursor().First()
		if k == nil {
			return nil
		}

		t, err := time.Parse(time.RFC3339Nano, string(k))
		if err != nil {
			return err
		}

		if t.After(time.Now().UTC()) {
			return nil
		}

		msg = decode(v)
		err = b.Delete(k)
		if err != nil {
			return err
		}

		// key needs to be cloned, k is not valid outside of the transaction
		key = append(key, k...)

		// stick things into outgoing bucket
		b = tx.Bucket(outgoingBucket)
		return b.Put(k, v)
	})

	return key, msg, err
}

// Pops multiple messages off the queue, until max or error is reached or the queue is empty
func (q *EmailQ) PopBatch(max int) (keys [][]byte, messages []*Msg, returnErr error) {
	for len(keys) < max {
		key, msg, err := q.Pop()
		if err != nil {
			returnErr = err
			break
		}

		if key == nil {
			break
		}

		keys = append(keys, key)
		messages = append(messages, msg)
	}

	return
}

// Recover re-queues outgoing emails that were interrupted
func (q *EmailQ) Recover() error {
	return q.db.Update(func(tx *bolt.Tx) error {
		outgoing := tx.Bucket(outgoingBucket)
		incoming := tx.Bucket(incomingBucket)

		c := outgoing.Cursor()

		for k, v := c.First(); k != nil; k, v = c.First() {
			err := c.Delete() // delete from outgoing
			if err != nil {
				return nil
			}

			// reinsert into incoming
			key := []byte(time.Now().UTC().Format(time.RFC3339Nano))

			incoming.Put(key, v)
		}

		return nil
	})
}

// RemoveDelivered removes successfully delivered message
func (q *EmailQ) RemoveDelivered(key []byte) error {
	return q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(outgoingBucket)
		return b.Delete(key)
	})
}

func decode(b []byte) *Msg {
	var result Msg
	buf := bytes.NewBuffer(b)
	decoder := gob.NewDecoder(buf)
	decoder.Decode(&result)
	return &result
}

func encode(msg *Msg) []byte {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	encoder.Encode(msg)

	return buf.Bytes()
}
