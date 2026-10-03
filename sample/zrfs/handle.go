package zrfs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zugcat/zob"
	"github.com/zugcat/zrpc"
	"github.com/zugcat/zrpc/sample/seqpacket"
)

var (
	ErrNotDir      = errors.New("not a directory")
	ErrNotReaderAt = errors.New("file not io.ReaderAt")
)

var (
	nextSTag = atomic.Uint64{}
)

type DirEntry struct {
	Name string
	Type fs.FileMode
}

type FileInfo struct {
	Size    int64
	Mode    fs.FileMode
	ModTime time.Time
}

type Handle interface {
	ReadAt(offset int64, length int) ([]byte, error)
	ReadAtTo(offset int64, length int) (seqpacket.Addr, int, error)
	ReadDirAt(offset, length int) ([]DirEntry, error)
	ReadLink(filename string) (string, error)
	Open(filename string) (Handle, error)
	Fstat() (*FileInfo, error)
	Lstat(filename string) (*FileInfo, error)
	Stat(filename string) (*FileInfo, error)
}

type HandleObject struct {
	Handle
}

func init() {
	zob.RegisterType[FileInfo]()
	zob.RegisterType[DirEntry]()
	zob.RegisterType[fs.PathError]()
	zob.RegisterType[fs.FileMode]()
	zob.RegisterType[seqpacket.Addr]()

	zob.RegisterType[HandleObject]()

	zob.RegisterError(ErrNotDir, "github.com/zugcat/zrpc/sample/zrfs.ErrNotDir")
	zob.RegisterError(ErrNotReaderAt, "github.com/zugcat/zrpc/sample/zrfs.ErrNotReaderAt")

	zob.RegisterError(fs.ErrClosed, "io/fs.ErrClosed")
	zob.RegisterError(fs.ErrExist, "io/fs.ErrExit")
	zob.RegisterError(fs.ErrInvalid, "io/fs.ErrInvalid")
	zob.RegisterError(fs.ErrNotExist, "io/fs.ErrNotExist")
	zob.RegisterError(fs.ErrPermission, "io/fs.ErrPermission")
}

type handle struct {
	*zrpc.Object
	es      *seqpacket.PacketConn
	lck     *sync.Mutex
	up      *handle
	fsys    fs.FS // if dir
	file    fs.File
	info    *FileInfo
	handles map[string]*handle
	dirents []DirEntry
}

var _ Handle = (*handle)(nil)

func fileInfo(info fs.FileInfo) *FileInfo {
	return &FileInfo{
		Size:    info.Size(),
		Mode:    info.Mode(),
		ModTime: info.ModTime(),
	}
}

func newRootHandle(e *seqpacket.PacketConn, fsys fs.FS) *handle {
	h := &handle{
		es:   e,
		fsys: fsys,
	}
	h.init()
	h.dirents = h.readDirents(".")
	return h
}

func newHandle(up *handle, file fs.File) *handle {
	h := &handle{
		es:   up.es,
		up:   up,
		file: file,
	}
	h.init()
	return h
}

func (h *handle) init() {
	h.lck = &sync.Mutex{}
	h.handles = make(map[string]*handle)

	if h.file == nil {
		h.file, _ = h.fsys.Open(".")
	}

	if h.file != nil {
		if info, err := h.file.Stat(); err == nil {
			h.info = fileInfo(info)
		} else {
			h.info = &FileInfo{}
		}
	}

	h.Object = zrpc.NewServerObject(h)
}

func (h *handle) readDirents(name string) []DirEntry {
	entries, _ := fs.ReadDir(h.fsys, name)
	dirents := make([]DirEntry, len(entries))

	for n, e := range entries {
		dirents[n] = DirEntry{
			Name: e.Name(),
			Type: e.Type(),
		}
	}
	return dirents
}

func (h *handle) open(filepath string) (*handle, error) {
	var name string
	var err error

	for {
		// path traversal
		dir := false
		name, filepath, _ = strings.Cut(filepath, "/")
		switch name {
		case "":
			for h.up != nil {
				h = h.up
			}
			dir = true

		case ".":
			dir = true

		case "..":
			if h.up == nil {
				return nil, fs.ErrNotExist
			}
			h = h.up
			dir = true

		default:
			if filepath != "" {
				h, err = h.open(name)
				if err != nil {
					err = fmt.Errorf("%s: %w", name, err)
					return nil, err
				}
			}
		}

		if filepath == "" {
			if dir {
				return h, nil
			}
			break
		}
	}

	h.lck.Lock()
	defer h.lck.Unlock()

	fh, ok := h.handles[name]
	if !ok {
		if h.fsys == nil {
			return nil, fmt.Errorf("%w: %w", fs.ErrInvalid, ErrNotDir)
		}

		file, err := h.fsys.Open(name)
		if err != nil {
			return nil, err
		}

		fh = newHandle(h, file)
		if fsys, err := fs.Sub(h.fsys, name); err == nil {
			fh.fsys = fsys
			fh.dirents = h.readDirents(name)
		}

		h.handles[name] = fh
	}

	return fh, nil
}

func (h *handle) Open(filepath string) (Handle, error) {
	if h.fsys == nil {
		return HandleObject{}, fmt.Errorf("%w: %w", fs.ErrInvalid, ErrNotDir)
	}

	fh, err := h.open(filepath)
	if err != nil {
		return HandleObject{}, err
	}

	return HandleObject{fh}, nil
}

func (h *handle) Fstat() (*FileInfo, error) {
	return h.info, nil
}

func (h *handle) Stat(filepath string) (*FileInfo, error) {
	if h.fsys == nil {
		return nil, fmt.Errorf("%w: %w", fs.ErrInvalid, ErrNotDir)
	}

	info, err := fs.Stat(h.fsys, filepath)
	if err != nil {
		return nil, err
	}

	return fileInfo(info), nil
}

func (h *handle) Lstat(filepath string) (*FileInfo, error) {
	if h.fsys == nil {
		return nil, fmt.Errorf("%w: %w", fs.ErrInvalid, ErrNotDir)
	}

	info, err := fs.Lstat(h.fsys, filepath)
	if err != nil {
		return nil, err
	}

	return fileInfo(info), nil
}

func (h *handle) ReadLink(filepath string) (string, error) {
	if h.fsys == nil {
		return "", fmt.Errorf("%w: %w", fs.ErrInvalid, ErrNotDir)
	}

	return fs.ReadLink(h.fsys, filepath)
}

func (h *handle) ReadDirAt(offset, length int) ([]DirEntry, error) {
	if h.fsys == nil {
		return nil, fmt.Errorf("%w: %w", fs.ErrInvalid, ErrNotDir)
	}
	if length <= 0 {
		length = len(h.dirents)
	}

	return h.dirents[offset:min(len(h.dirents), offset+length)], nil
}

func (h *handle) ReadAt(offset int64, length int) ([]byte, error) {
	rd, ok := h.file.(io.ReaderAt)
	if !ok {
		return nil, ErrNotReaderAt
	}

	length = min(length, 1024)
	buf := make([]byte, length)

	n, err := rd.ReadAt(buf, offset)
	return buf[:max(n, 0)], err
}

func (h *handle) ReadAtTo(offset int64, length int) (seqpacket.Addr, int, error) {
	rd, ok := h.file.(io.ReaderAt)
	if !ok {
		return 0, 0, ErrNotReaderAt
	}

	bytes := make([]byte, length)
	nr, err := rd.ReadAt(bytes, offset)
	if nr <= 0 {
		return 0, nr, err
	}

	stag := seqpacket.Addr(nextSTag.Add(1))
	buf, err2 := h.es.Endpoint().Buffer(stag, int64(nr))
	if err2 != nil {
		return 0, 0, err2
	}

	if !buf.Local(bytes[:nr]) {
		buf.Write(bytes[:nr])
	}

	return stag, nr, err
}
