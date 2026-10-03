package zrfs_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zugcat/zrpc/sample/zrfs"
	"github.com/zugcat/zrpc/sample/zrfs/zrfsclient"
)

func makePair(ctx context.Context, t testing.TB, fsys fs.FS) (zrfsclient.FS, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)

	sr, cw := io.Pipe()
	cr, sw := io.Pipe()

	conn := zrfs.New(sr, sw, fsys)
	client := zrfsclient.New(cr, cw)

	errC := make(chan error, 1)
	go func() { errC <- conn.Run(ctx) }()

	return client, func() {
		cancel()
		sr.Close()
		sw.Close()
		cr.Close()
		cw.Close()

		err := <-errC
		if errors.Is(err, io.ErrClosedPipe) {
			err = nil
		}
		assert.NoError(t, err)
	}
}

func walk(t testing.TB, fsys fs.FS) string {
	out := &strings.Builder{}

	err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		require.NoError(t, err)
		if err != nil {
			return err
		}

		info, err := d.Info()
		assert.NoError(t, err)
		fmt.Fprintln(out, info)

		if d.IsDir() {
			return nil
		}

		contents, err := fs.ReadFile(fsys, path)
		require.NoError(t, err)
		fmt.Fprintln(out, string(contents))

		return err
	})
	require.NoError(t, err)

	return out.String()
}

func TestSimple(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	now := time.Now().UTC()
	fsys := fstest.MapFS{
		"hello.txt": {
			Data:    []byte("this is a test\n"),
			Mode:    0557,
			ModTime: now,
		},
		"subdir/file.txt": {
			Data:    []byte("contents of file\n"),
			Mode:    0557,
			ModTime: now,
		},
		"subdir2": {
			Mode:    fs.ModeDir | 0555,
			ModTime: now,
		},
		"subdir3/link": {
			Mode:    fs.ModeSymlink | 0777,
			ModTime: now,
			Data:    []byte("../subdir/file.txt"),
		},
	}

	client, stop := makePair(ctx, t, fsys)
	defer stop()

	local := walk(t, fsys)
	remote := walk(t, client)

	assert.Equal(t, local, remote)
	t.Log(string(remote))
}

func hugeFileFS(t testing.TB) (fs.FS, []byte) {
	buf := &bytes.Buffer{}
	length := 0x17_ff23
	_, err := io.Copy(buf, io.LimitReader(rand.Reader, int64(length)))
	require.NoError(t, err)

	fsys := fstest.MapFS{
		"hugefile": {
			Data: buf.Bytes(),
			Mode: 0557,
		},
	}

	return fsys, buf.Bytes()
}

type dumbReader struct {
	io.Reader
}

func TestHugeFile(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	fsys, buf := hugeFileFS(t)

	client, stop := makePair(ctx, t, fsys)
	defer stop()

	fd, err := client.Open("hugefile")
	require.NoError(t, err)
	defer fd.Close()

	buf2 := &bytes.Buffer{}
	nw, err := io.Copy(buf2, fd)
	require.NoError(t, err)

	assert.EqualValues(t, len(buf), nw)
	assert.True(t, bytes.Equal(buf, buf2.Bytes()))

	fd.(io.Seeker).Seek(0, io.SeekStart)
	buf2.Reset()

	nw, err = io.Copy(buf2, dumbReader{fd})

	require.NoError(t, err)

	assert.EqualValues(t, len(buf), nw)
	assert.True(t, bytes.Equal(buf, buf2.Bytes()))
}

func BenchmarkHugeFile(b *testing.B) {
	ctx, cancel := context.WithCancel(b.Context())
	defer cancel()

	fsys, _ := hugeFileFS(b)

	client, stop := makePair(ctx, b, fsys)
	defer stop()

	b.Run("mapfs", func(b *testing.B) {
		for b.Loop() {
			fd, err := fsys.Open("hugefile")
			if err != nil {
				b.Fatal(err)
			}
			io.Copy(io.Discard, fd)
			fd.Close()
		}
	})

	b.Run("client", func(b *testing.B) {
		for b.Loop() {
			fd, err := client.Open("hugefile")
			if err != nil {
				b.Fatal(err)
			}
			io.Copy(io.Discard, fd)
			fd.Close()
		}
	})
}
