package emailq

import (
	"bytes"
	"encoding/gob"
	"time"

	"github.com/boltdb/bolt"
)

var (
	incomingBucket = []byte("incoming")
)

type EmailQ struct {
	db *bolt.DB
}

type Msg struct {
	Host string
	From string
	To   []string
	Data []byte
}

// Creates new instance of EmailQ
func New(filepath string) (*EmailQ, error) {
	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		return nil, err
	}

	// create incoming bucket
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(incomingBucket)
		return err
	})

	if err != nil {
		return nil, err
	}

	return &EmailQ{
		db: db,
	}, nil
}

func (q *EmailQ) Close() error {
	return q.db.Close()
}

func (q *EmailQ) Push(msg *Msg) error {
	key := []byte(time.Now().UTC().Format(time.RFC3339Nano))
	value := encode(msg)

	err := q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(incomingBucket)
		return b.Put(key, value)
	})

	return err
}

func (q *EmailQ) Pop() (msg *Msg, err error) {
	err = q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(incomingBucket)
		k, v := b.Cursor().First()
		if k == nil {
			return nil
		}

		msg = decode(v)
		return b.Delete(k)
	})

	return msg, err
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
