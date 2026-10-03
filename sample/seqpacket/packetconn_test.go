package seqpacket_test

import (
	"bytes"
	"embed"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zugcat/zrpc/sample/seqpacket"
)

func makePair() (*seqpacket.Endpoint, *seqpacket.Endpoint) {
	r1, w2 := io.Pipe()
	r2, w1 := io.Pipe()

	return seqpacket.NewEndpoint(r1, w1), seqpacket.NewEndpoint(r2, w2)
}

func TestBasicSeq(t *testing.T) {
	e1, e2 := makePair()
	defer e1.Close()
	defer e2.Close()

	c1 := e1.Dial(seqpacket.Addr(1), seqpacket.Addr(2))
	c2 := e2.Listen(seqpacket.Addr(2))

	wg := sync.WaitGroup{}
	wg.Go(func() {
		for {
			buf := make([]byte, 10)
			n, from, err := c2.ReadFrom(buf)
			if errors.Is(err, io.EOF) {
				break
			}

			require.NoError(t, err)
			assert.Equal(t, c1.LocalAddr(), from)
			assert.Equal(t, 4, n)
			assert.Equal(t, []byte("abcd"), buf[:n])
		}
	})

	n, err := c1.Write([]byte("abcd"))
	require.NoError(t, err)
	require.Equal(t, 4, n)

	err = c1.Close()
	require.NoError(t, err)

	wg.Wait()
}

func TestBusySeq(t *testing.T) {
	const N = 1000

	e1, e2 := makePair()
	defer e1.Close()
	defer e2.Close()

	cc1, cc2 := make([]*seqpacket.PacketConn, N), make([]*seqpacket.PacketConn, N)
	for n := range N {
		a1, err := seqpacket.Resolve(strconv.Itoa(n))
		require.NoError(t, err)
		a2, err := seqpacket.Resolve(strconv.Itoa(n + N))
		require.NoError(t, err)
		c1, c2 := e1.Listen(a1), e2.Listen(a2)
		cc1[n], cc2[n] = c1, c2
	}

	wg := sync.WaitGroup{}

	for n := range N {
		c1, c2 := cc1[n], cc2[n]

		wg.Go(func() {
			buf := make([]byte, 128)
			n, from, err := c1.ReadFrom(buf)
			if errors.Is(err, io.ErrClosedPipe) {
				return
			}
			require.NoError(t, err)
			buf = buf[:n]

			var s string
			n, err = fmt.Sscanf(string(buf), "hello from %s", &s)
			require.NoError(t, err)
			require.Equal(t, 1, n)

			assert.Equal(t, from.String(), s)
		})

		wg.Go(func() {
			a, err := seqpacket.Resolve(strconv.Itoa(n))
			require.NoError(t, err)

			s := fmt.Sprintf("hello from %v", c2.LocalAddr())
			n, err := c2.WriteTo([]byte(s), a)
			require.NoError(t, err)
			assert.Len(t, s, n)
		})
	}

	wg.Wait()
}

//go:embed packetconn_test.go
var textFile embed.FS

func TestSend(t *testing.T) {
	e1, e2 := makePair()
	defer e1.Close()
	defer e2.Close()

	c1, c2 := e1.Dial(0, 0), e2.Dial(0, 0)

	defer func(x int) { seqpacket.MaxSendBuffer = x }(seqpacket.MaxSendBuffer)
	seqpacket.MaxSendBuffer = 64

	fd, err := textFile.Open("packetconn_test.go")
	require.NoError(t, err)
	defer fd.Close()
	info, err := fd.Stat()
	require.NoError(t, err)

	dtag := seqpacket.Addr(6)
	dbuf, err := e2.Buffer(dtag, info.Size())
	require.NoError(t, err)

	sbytes := &bytes.Buffer{}
	stag := seqpacket.Addr(5)
	sbuf, err := e1.Buffer(stag, info.Size())
	require.NoError(t, err)

	nc, err := io.Copy(io.MultiWriter(sbuf, sbytes), fd)
	assert.Equal(t, info.Size(), nc)

	wg := sync.WaitGroup{}
	wg.Go(func() {
		err := e2.ReadBuffer(dbuf, stag, 0, int(info.Size()), true)
		require.NoError(t, err)

		c2.Close()
	})

	_, err = c1.Read([]byte{0})
	require.ErrorIs(t, err, io.EOF)
	wg.Wait()

	dbuf.SetOffset(0)
	dbytes, err := io.ReadAll(dbuf)
	require.NoError(t, err)

	assert.Equal(t, sbytes.String(), string(dbytes))
}
