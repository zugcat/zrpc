package zrpc_test

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zugcat/zob"
	"github.com/zugcat/zrpc"
)

type closeReader chan struct{}

func (c closeReader) Read([]byte) (int, error) {
	<-c
	return 0, io.EOF
}

func (c closeReader) Close() error {
	close(c)
	return nil
}

func TestServerUpDown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cr := make(closeReader)
	cx := zob.DefaultCodec(cr, io.Discard)
	s := zrpc.NewServer(cx, Foo(0), 100)

	errC := make(chan error)
	go func() { errC <- s.Run(ctx) }()

	cr.Close()
	assert.ErrorIs(t, <-errC, io.EOF)
}
