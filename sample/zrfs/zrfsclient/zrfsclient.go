package zrfsclient

import (
	"errors"
	"io"
	"io/fs"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zugcat/zob"
	"github.com/zugcat/zrpc"
	"github.com/zugcat/zrpc/sample/seqpacket"
	"github.com/zugcat/zrpc/sample/zrfs"
)

//go:generate go run ../../../cmd/zrpcgen -out handle_gen.go Handle .. ../../seqpacket

type FS interface {
	fs.FS
	fs.StatFS
	fs.SubFS
	fs.ReadLinkFS
}

type File interface {
	fs.FS
	fs.File
	fs.ReadDirFile
	fs.ReadLinkFS
	io.ReaderAt
	io.Seeker
	io.WriterTo
	StatFrom(string) (fs.FileInfo, error)
}

type FileInfo struct {
	name string
	*zrfs.FileInfo
}

func (f FileInfo) String() string     { return fs.FormatFileInfo(f) }
func (f FileInfo) Name() string       { return f.name }
func (f FileInfo) Size() int64        { return f.FileInfo.Size }
func (f FileInfo) Mode() fs.FileMode  { return f.FileInfo.Mode }
func (f FileInfo) ModTime() time.Time { return f.FileInfo.ModTime }
func (f FileInfo) IsDir() bool        { return f.FileInfo.Mode.IsDir() }
func (f FileInfo) Sys() any           { return nil }

type DirEntry struct {
	file *file
	zrfs.DirEntry
}

func (d DirEntry) String() string    { return fs.FormatDirEntry(d) }
func (d DirEntry) Name() string      { return d.DirEntry.Name }
func (d DirEntry) Type() fs.FileMode { return d.DirEntry.Type }
func (d DirEntry) IsDir() bool       { return d.DirEntry.Type.IsDir() }

func (d DirEntry) Info() (fs.FileInfo, error) {
	return d.file.Lstat(d.DirEntry.Name)
}

var (
	_ File        = (*file)(nil)
	_ FS          = (*client)(nil)
	_ fs.FileInfo = FileInfo{}
	_ fs.DirEntry = DirEntry{}
)

var nextDTag = atomic.Uint64{}

func New(rd io.ReadCloser, wr io.Writer) FS {
	e := seqpacket.NewEndpoint(rd, wr)
	es := e.Dial(0, 0)

	cx := zob.DefaultCodec(es, es, zob.AutoFlush(false))

	var c *zrpc.Client

	c = zrpc.NewClient(cx,
		zob.WithMarshalers(
			zob.MarshalToFunc(func(m zob.Marshaler, h zrfs.HandleObject) error {
				return m.Encode(h.Handle.(HandleStub).Object.ID())
			}),
		),
		zob.WithUnmarshalers(
			zob.UnmarshalFromFunc(func(u zob.Unmarshaler, h *zrfs.HandleObject) error {
				var id uint64

				if err := u.Decode(&id); err != nil {
					return err
				}

				h.Handle = HandleStub{zrpc.NewClientObject(id, c)}
				return nil
			}),
		),
	)

	return &client{
		Client: c,
		es:     es,
	}
}

type client struct {
	*zrpc.Client
	es      *seqpacket.PacketConn
	lck     sync.Mutex
	root    *file
	bufAddr atomic.Uint64
}

type file struct {
	*client
	lck     sync.Mutex
	pointer int64
	info    FileInfo
	handle  zrfs.Handle
	closed  bool
}

func (f *file) Open(pathname string) (fs.File, error) {
	fh, err := f.handle.Open(pathname)
	if err != nil {
		return nil, err
	}
	return f.newFile(fh, path.Base(pathname)), nil
}

func (f *file) Close() error {
	f.lck.Lock()
	defer f.lck.Unlock()

	if f.closed {
		return fs.ErrClosed
	}
	f.closed = true

	return nil
}

// stat presumes f is locked
func (f *file) stat() (fs.FileInfo, error) {
	if f.info.FileInfo == nil {
		info, err := f.handle.Fstat()
		if err != nil {
			return nil, err
		}
		f.info.FileInfo = info
	}

	return f.info, nil
}

func (f *file) Stat() (fs.FileInfo, error) {
	f.lck.Lock()
	defer f.lck.Unlock()

	return f.stat()
}

func (f *file) StatFrom(pathname string) (fs.FileInfo, error) {
	info, err := f.handle.Stat(pathname)
	if err != nil {
		return nil, err
	}

	return FileInfo{name: path.Base(pathname), FileInfo: info}, nil
}

func (f *file) Lstat(pathname string) (fs.FileInfo, error) {
	info, err := f.handle.Lstat(pathname)
	if err != nil {
		return nil, err
	}

	return FileInfo{name: path.Base(pathname), FileInfo: info}, nil
}

// asEOF normalizes io.EOF, as too many things depend on io.EOF being equally comparable.
func asEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.EOF
	}

	return err
}

func (f *file) Read(p []byte) (int, error) {
	f.lck.Lock()
	defer f.lck.Unlock()

	n, err := f.ReadAt(p, f.pointer)
	if n > 0 {
		f.pointer += int64(n)
	}

	return n, asEOF(err)
}

func (f *file) ReadAt(p []byte, off int64) (int, error) {
	if len(p) <= 1024 {
		bytes, err := f.handle.ReadAt(off, len(p))
		return copy(p, bytes), asEOF(err)
	}

	dtag := seqpacket.Addr(nextDTag.Add(1))
	buf, err := f.es.Endpoint().Buffer(dtag, int64(len(p)))
	if err != nil {
		return 0, err
	}
	defer f.es.Endpoint().Invalidate(buf)

	local := buf.Local(p)

	n, err := f.readAt(buf, off, len(p))

	if !local {
		buf.SetOffset(0)
		buf.Write(p)
	}

	return n, err
}

func (f *file) readAt(buf *seqpacket.Buffer, off int64, length int) (int, error) {
	stag, n, err := f.handle.ReadAtTo(off, length)
	err = asEOF(err)
	if n <= 0 {
		return n, err
	}

	if err := f.es.Endpoint().ReadBuffer(buf, stag, 0, n, true); err != nil {
		return 0, err
	}

	return n, err
}

func (f *file) WriteTo(w io.Writer) (nr int64, err error) {
	f.lck.Lock()
	defer f.lck.Unlock()

	const chunkSize = 2 * 1024 * 1024

	done := false

	dtag := seqpacket.Addr(nextDTag.Add(1))
	buf, err := f.es.Endpoint().Buffer(dtag, chunkSize)
	if err != nil {
		return 0, err
	}
	defer f.es.Endpoint().Invalidate(buf)

	for err == nil && !done {
		var n int
		var nw int64

		n, err = f.readAt(buf, f.pointer, chunkSize)

		if errors.Is(err, io.EOF) {
			done = true
			err = nil
		} else if err != nil {
			break
		}

		if n > 0 {
			buf.SetOffset(0)
			nw, err = buf.WriteTo(w)
			if nw > 0 {
				nr += nw
				f.pointer += nw
			}
		} else {
			done = true
		}
	}

	return nr, err
}

func (f *file) ReadDir(n int) ([]fs.DirEntry, error) {
	f.lck.Lock()
	defer f.lck.Unlock()

	entries, err := f.handle.ReadDirAt(int(f.pointer), n)
	if err != nil {
		return nil, err
	}
	f.pointer += int64(len(entries))

	fse := make([]fs.DirEntry, len(entries))
	for n, e := range entries {
		fse[n] = DirEntry{file: f, DirEntry: e}
	}

	if n > 0 && len(entries) < n {
		err = io.EOF
	}

	return fse, err
}

func (f *file) Seek(offset int64, whence int) (int64, error) {
	f.lck.Lock()
	defer f.lck.Unlock()

	switch whence {
	case io.SeekStart:
		if offset < 0 {
			return f.pointer, fs.ErrInvalid
		}
		f.pointer = offset

	case io.SeekCurrent:
		if p := f.pointer + offset; p < 0 {
			return f.pointer, fs.ErrInvalid
		} else {
			f.pointer = p
		}

	case io.SeekEnd:
		info, err := f.stat()
		if err != nil {
			return f.pointer, err
		}
		if p := info.Size() + offset; p < 0 {
			return f.pointer, fs.ErrInvalid
		} else {
			f.pointer = p
		}

	default:
		return f.pointer, fs.ErrInvalid
	}

	return f.pointer, nil
}

func (c *client) newFile(fh zrfs.Handle, name string) *file {
	f := &file{
		client: c,
		handle: fh,
		info:   FileInfo{name: name},
	}

	return f
}

func (c *client) getRoot() error {
	c.lck.Lock()
	defer c.lck.Unlock()

	if c.root == nil {
		o, err := c.Object()
		if err != nil {
			return err
		}

		c.root = c.newFile(HandleStub{o}, "")
	}

	return nil
}

func (c *client) Open(pathname string) (fs.File, error) {
	if err := c.getRoot(); err != nil {
		return nil, err
	}

	return c.root.Open(pathname)
}

func (c *client) Sub(pathname string) (fs.FS, error) {
	if err := c.getRoot(); err != nil {
		return nil, err
	}

	root, err := c.root.Open(pathname)
	if err != nil {
		return nil, err
	}

	return &client{es: c.es, root: root.(*file)}, nil
}

func (c *client) ReadLink(pathname string) (string, error) {
	if err := c.getRoot(); err != nil {
		return "", err
	}

	return c.root.handle.ReadLink(pathname)
}

func (c *client) Lstat(pathname string) (fs.FileInfo, error) {
	if err := c.getRoot(); err != nil {
		return nil, err
	}

	return c.root.Lstat(pathname)
}

func (c *client) Stat(pathname string) (fs.FileInfo, error) {
	if err := c.getRoot(); err != nil {
		return nil, err
	}

	return c.root.StatFrom(pathname)
}
