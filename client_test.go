package zrpc_test

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zugcat/zob"
	"github.com/zugcat/zrpc"
)

func serverClient(ctx context.Context, t testing.TB, x any) (*zrpc.Object, func()) {
	cr, sw := io.Pipe()
	sr, cw := io.Pipe()

	ccx := zob.DefaultCodec(cr, cw, zob.AutoFlush(false))
	scx := zob.DefaultCodec(sr, sw, zob.AutoFlush(false))

	server := zrpc.NewServer(scx, x, 10)
	client := zrpc.NewClient(ccx)

	errC := make(chan error)
	go func() { errC <- server.Run(ctx) }()

	o, err := client.Object()
	require.NoError(t, err)

	return o, func() {
		cr.Close()
		sr.Close()
		cw.Close()
		sw.Close()

		assert.ErrorIs(t, <-errC, io.ErrClosedPipe)
		assert.ErrorIs(t, client.Wait(), io.ErrClosedPipe)
	}
}

func TestClient(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	o, stop := serverClient(ctx, t, Foo(0))
	defer stop()

	t.Logf("root object id=%d", o.ID())

	t.Run("add", func(t *testing.T) {
		r, err := o.Call("Add", 2, 3)
		require.NoError(t, err)
		assert.Equal(t, 5, r[0])
	})

	t.Run("reverse", func(t *testing.T) {
		r, err := o.Call("Reverse", []int{1, 2, 3, 4, 5})
		require.NoError(t, err)
		assert.Equal(t, []int{5, 4, 3, 2, 1}, r[0])
	})

	t.Run("error", func(t *testing.T) {
		r, err := o.Call("Error")
		require.NoError(t, err)
		assert.ErrorIs(t, r[0].(error), errNope)
	})
}
