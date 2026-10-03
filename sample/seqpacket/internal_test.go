package seqpacket

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func readSeg(t testing.TB, r io.Reader) []byte {
	t.Helper()
	buf := &bytes.Buffer{}
	_, err := io.Copy(buf, r)
	require.NoError(t, err)
	return buf.Bytes()
}

func TestFragmentedBuffer(t *testing.T) {
	buf, err := newBuffer(0, 16)
	require.NoError(t, err)
	w, err := buf.Write([]byte("abcd"))
	require.NoError(t, err)
	assert.Equal(t, 4, w)
	assert.EqualValues(t, 4, buf.len)

	buf.SetOffset(0)
	var bytes [16]byte

	r, err := buf.Read(bytes[:])
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 4, r)
	assert.Equal(t, "abcd", string(bytes[:4]))

	buf.SetOffset(2)
	buf.Write([]byte("1234"))

	buf.SetOffset(0)
	r, err = buf.Read(bytes[:])
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 6, r)
	assert.Equal(t, "ab1234", string(bytes[:6]))

	w, err = buf.Write([]byte("ABCD"))
	require.NoError(t, err)
	assert.Equal(t, w, 4)
	assert.EqualValues(t, 10, buf.len)

	buf.SetOffset(0)
	r, err = buf.Read(bytes[:])
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 10, r)
	assert.Equal(t, "ab1234ABCD", string(bytes[:10]))

	buf.SetOffset(2)
	w, err = buf.Write([]byte("xy"))
	require.NoError(t, err)
	assert.Equal(t, 2, w)

	buf.SetOffset(0)
	r, err = buf.Read(bytes[:])
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 10, r)
	assert.Equal(t, "abxy34ABCD", string(bytes[:10]))
}

type dumbReader struct {
	io.Reader
}

func TestSegSize(t *testing.T) {
	buf, err := newBuffer(0, 64)
	require.NoError(t, err)

	contents := make([]byte, 64)
	for n := range contents {
		contents[n] = byte(n)
	}

	w, err := buf.Write(contents)
	require.NoError(t, err)
	assert.Equal(t, 64, w)

	buf.SetOffset(2)
	buf.seg = 4

	rc := readSeg(t, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte{2, 3, 4, 5}, rc)

	rc = readSeg(t, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte{6, 7, 8, 9}, rc)

	buf.SetOffset(62)
	rc = readSeg(t, buf)
	assert.Equal(t, []byte{62, 63}, rc)

	buf.SetOffset(2)
	_, err = io.Copy(buf, dumbReader{bytes.NewReader([]byte{128, 129, 130, 131, 132, 133, 134, 150})})
	require.NoError(t, err)

	buf.SetOffset(2)
	buf.seg = 8
	rc = readSeg(t, buf)
	assert.Equal(t, []byte{128, 129, 130, 131, 6, 7, 8, 9}, rc)
}
