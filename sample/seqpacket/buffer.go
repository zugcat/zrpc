package seqpacket

import (
	"errors"
	"io"
	"net"
	"slices"
)

var (
	ErrBadAck  = errors.New("invalid ack message")
	ErrBadSize = errors.New("bad size")
)

// Buffer represents a remote buffer.
// It is interfaced in a way it could be implemented via linux spice,
// an RDMA buffer, hopefully any other kernel memory interface, or,
// in this case, local slices passed around.
//
// In this implementation, net.Buffers are leveraged which use underlying
// readv/writev syscalls if the OS supports it.
//
// It is not safe to manipulate the same Buffer instance concurrently from
// more than one go routine at a time.
type Buffer struct {
	tag           Addr
	data          net.Buffers
	len, cap, off int64
	seg, idx      int
	local         bool
}

// invariants after a read or write:
// - b.idx is pointing at the b.data index for the next read or write,
//   which may be len(b.data) to indicate at end
// - if b.idx is < len(b.data), the start of this buffer is the read
//   or write position

func splitBuffers(data net.Buffers, size int64) (left net.Buffers, right net.Buffers) {
	for _, b := range data {
		if size > 0 {
			lenb := int64(len(b))

			switch {
			case lenb > size:
				left = append(left, b[:size])
				right = append(right, b[size:])
				size = 0

			case lenb < size:
				left = append(left, b)
				size -= lenb

			case lenb == size:
				left = append(left, b)
				size = 0
			}
		} else {
			right = append(right, b)
		}
	}

	return left, right
}

// newBuffer creates a new buffer.
func newBuffer(tag Addr, cap int64) (*Buffer, error) {
	return &Buffer{
		tag: tag,
		cap: cap,
	}, nil
}

// Tag returns the address this buffer was assigned
func (b *Buffer) Tag() Addr { return b.tag }

// SetOffset sets the next read or write position
func (b *Buffer) SetOffset(off int64) error {
	if off < 0 || off >= b.cap {
		return ErrBounds
	}

	if off == 0 {
		b.idx = 0
	} else if off > b.len {
		b.data = append(b.data, make([]byte, off-b.len))
		b.idx = len(b.data)
		b.len = off
	} else {
		left, right := splitBuffers(b.data, off)
		b.idx = len(left)
		b.data = append(left, right...)
	}

	b.off = off
	return nil
}

// Local sets a local backing if this is not a kernel buffer.
// Returns true if the slice was accepted.
func (b *Buffer) Local(p []byte) bool {
	n := min(b.cap-b.len, int64(len(p)))
	p = p[:n]
	if len(p) > 0 {
		b.data = append(b.data, p)
		b.len += int64(len(p))
		b.local = true
		return true
	}

	return false
}

// segLen returns a length s capped by segment and remaining capacity from the offset
func (b *Buffer) segLen(s int64) int64 {
	s = min(s, b.cap-b.off)
	if b.seg > 0 {
		s = min(s, int64(b.seg))
	}
	return s
}

// Write writes user memory to the buffer.
// The slice written is kept by reference and should be considered
// owned by the buffer.
func (b *Buffer) Write(p []byte) (nw int, err error) {
	nw = len(p)

	w := nw
	for w > 0 && b.idx < len(b.data) {
		d := b.data[b.idx:]
		n := copy(d[0], p)
		p = p[n:]
		w -= n

		if n < len(d[0]) {
			data := append(b.data[:b.idx], d[0][:n], d[0][n:])
			data = append(data, d[1:]...)
			b.data = data
		}

		b.idx++
	}

	if w > 0 {
		b.data = append(b.data, p)
		b.len += int64(len(p))
		b.idx++
	}

	b.off += int64(nw)
	return nw, nil
}

// Read copies from the buffer to user memory
func (b *Buffer) Read(p []byte) (nr int, err error) {
	left, right := splitBuffers(b.data[b.idx:], int64(len(p)))
	lr := slices.Clone(left)
	nr, _ = lr.Read(p)
	if len(right) == 0 {
		err = io.EOF
	}

	data := b.data[:b.idx]
	data = append(data, left...)
	data = append(data, right...)
	b.data = data
	b.idx += len(left)
	b.off += int64(nr)

	return nr, err
}

// ReadFrom fills the buffer from the reader.
func (b *Buffer) ReadFrom(r io.Reader) (nr int64, err error) {
	rsize := b.segLen(b.cap)

	var n int
	for err == nil && rsize > 0 {
		if b.idx < len(b.data) {
			d := b.data[b.idx]
			s := min(int64(len(d)), rsize)
			n, err = r.Read(d[:s])
			if n > 0 {
				n64 := int64(n)
				nr += n64
				b.off += n64
				rsize -= nr

				if n < len(d) {
					left, right := splitBuffers(b.data[b.idx:], n64)
					data := append(b.data[:b.idx], left...)
					data = append(data, right...)
					b.data = data
				}
				b.idx++
			}
		} else {
			buf := make([]byte, min(0x8_0000, rsize))

			n, err = r.Read(buf)
			if n > 0 {
				n64 := int64(n)
				nr += n64
				rsize -= n64

				b.Write(buf[:n])
			}
		}
	}

	if errors.Is(err, io.EOF) {
		err = nil
	}
	return nr, err
}

// WriteTo writes the buffer to the writer.
func (b *Buffer) WriteTo(w io.Writer) (nw int64, err error) {
	var left, right net.Buffers
	if b.seg > 0 {
		left, right = splitBuffers(b.data[b.idx:], int64(b.seg))
	} else {
		left = b.data[b.idx:]
	}

	lr := slices.Clone(left)
	nw, err = io.Copy(w, &lr)
	data := b.data[:b.idx]
	data = append(data, left...)
	data = append(data, right...)
	b.data = data
	b.idx += len(left)

	if errors.Is(err, io.EOF) && (len(right) == 0 || (b.seg > 0 && int64(b.seg) == nw)) {
		err = nil
	}
	b.off += nw
	return nw, err
}
