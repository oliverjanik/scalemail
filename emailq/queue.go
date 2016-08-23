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
	retryBucket    = []byte("retry")
	deadBucket     = []byte("deadletter")
)

// EmailQ is a persistent queue that holds the mail messages
type EmailQ struct {
	db *bolt.DB
}

// Msg represents email message
type Msg struct {
	Host string
	From string
	To   []string
	Data []byte
}

// New creates new instance of EmailQ
func New(filepath string) (*EmailQ, error) {
	db, err := bolt.Open("emails.db", 0600, nil)
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

		_, err = tx.CreateBucketIfNotExists(retryBucket)
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

// PushRetry takes msg from outgoing queue and places that in the Retry queue
func (q *EmailQ) PushRetry(key []byte) error {
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

		retry := tx.Bucket(retryBucket)

		return retry.Put(key, msg)
	})
}

// PushDeadLetter takes msg out of outgoing and pushed that to Dead Letter queue
func (q *EmailQ) PushDeadLetter(key []byte) error {
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

// PopRetry fetches next item from Retry queue
func (q *EmailQ) PopRetry() (k []byte, msg *Msg, err error) {
	err = q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(retryBucket)

		var v []byte
		k, v = b.Cursor().First()
		if k == nil {
			return nil
		}

		msg = decode(v)
		err = b.Delete(k)
		if err != nil {
			return err
		}

		// stick things into outgoing bucket
		b = tx.Bucket(outgoingBucket)
		if b == nil {
			return fmt.Errorf("Outgoing bucket is nil")
		}

		return b.Put(k, v)
	})

	return k, msg, err
}

// PopIncoming get next email from the queue
func (q *EmailQ) PopIncoming() (k []byte, msg *Msg, err error) {
	err = q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(incomingBucket)

		var v []byte
		k, v = b.Cursor().First()
		if k == nil {
			return nil
		}

		msg = decode(v)
		err = b.Delete(k)
		if err != nil {
			return err
		}

		// stick things into outgoing bucket
		b = tx.Bucket(outgoingBucket)
		if b == nil {
			return fmt.Errorf("Outgoing bucket is nil")
		}

		return b.Put(k, v)
	})

	return k, msg, err
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
