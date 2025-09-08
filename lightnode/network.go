package lightnode

import (
	"context"
	"fmt"
)

type Header struct {
	Height int
	Header []byte
}

type Network interface {
	StreamHeaders(ctx context.Context) <-chan Header
}

type DummyNetwork struct {
	MaxHeight int
}

func (d DummyNetwork) StreamHeaders(ctx context.Context) <-chan Header {
	ch := make(chan Header)
	go func() {
		defer close(ch)
		for h := 1; h <= d.MaxHeight; h++ {
			select {
			case <-ctx.Done():
				return
			case ch <- Header{Height: h, Header: []byte(fmt.Sprintf("header-%d", h))}:
			}
		}
	}()
	return ch
}
