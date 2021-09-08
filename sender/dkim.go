package sender

import (
	"bytes"
	"crypto"
	"io"
	"log"

	"github.com/emersion/go-msgauth/dkim"
)

// Encapsulates dkim package signing with error handling
type Signer struct {
	domain   string
	selector string
	key      crypto.Signer
}

func NewSigner(domain, selector string, key crypto.Signer) *Signer {
	return &Signer{
		domain:   domain,
		selector: selector,
		key:      key,
	}
}

func (s *Signer) Sign(emailBuf []byte, w io.Writer) error {
	defer func() {
		if r := recover(); r != nil {
			log.Println("dkim.Sign panicked:", r)
		}
	}()

	r := bytes.NewReader(emailBuf)
	options := &dkim.SignOptions{
		Domain:   s.domain,
		Selector: s.selector,
		Signer:   s.key,
	}

	err := dkim.Sign(w, r, options)
	if err != nil {
		log.Println("Error signing email:", err)
	}

	return err
}
