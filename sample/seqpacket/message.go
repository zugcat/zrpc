package seqpacket

import (
	"encoding/binary"
	"io"
	"net"
	"slices"
	"unsafe"
)

type message struct {
	src, dst Addr
	seq      uint64
	soff     uint64
	doff     uint64
	len      uint32
	msgtype  uint8
	flags    uint8
	deadbeef uint16 // data starts at 64 bit aligned offset
	data     []byte
}

const messageLen = 0x30

const (
	normalMsg uint8 = iota
	closeMsg
	readMsg
	sendMsg
)

const (
	invalidateFlag uint8 = 1 << iota
)

func bytesFor[T any](x *T, length int) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(x)), length)
}

func (m *message) write(w io.Writer, buf *Buffer) (r int, err error) {
	mb := slices.Clone(bytesFor(m, messageLen))

	if buf != nil {
		buf.off = int64(m.soff)
		buf.seg = int(m.len)
	}

	binary.BigEndian.PutUint32(mb[unsafe.Offsetof(m.len):], m.len)
	binary.BigEndian.PutUint64(mb[unsafe.Offsetof(m.soff):], m.soff)
	binary.BigEndian.PutUint64(mb[unsafe.Offsetof(m.doff):], m.doff)

	bufs := net.Buffers{mb}
	if m.len > 0 && len(m.data) > 0 {
		bufs = append(bufs, m.data[:m.len])
	}

	_, err = io.Copy(w, &bufs)
	if err != nil {
		return 0, err
	}

	r = int(m.len)

	if buf != nil {
		var nr int64

		nr, err = io.Copy(w, buf)
		r = int(nr)
	}

	return r, err
}

func (m *message) read(r io.Reader, dst Addr, buf []byte) (n int, err error) {
	mb := bytesFor(m, messageLen)
	if _, err := io.ReadAtLeast(r, mb, messageLen); err != nil {
		return 0, err
	}
	m.len = binary.BigEndian.Uint32(mb[unsafe.Offsetof(m.len):])
	m.soff = binary.BigEndian.Uint64(mb[unsafe.Offsetof(m.soff):])
	m.doff = binary.BigEndian.Uint64(mb[unsafe.Offsetof(m.doff):])

	length := int(m.len)
	if length == 0 || m.msgtype != normalMsg {
		// no data, or caller handles the data
		return 0, nil
	}

	if length > MaxSendBuffer {
		return 0, ErrBounds
	}

	if m.dst == dst && buf != nil {
		m.data = m.data[:0]

		buflen := min(len(buf), length)
		if buflen > 0 {
			n, err = io.ReadAtLeast(r, buf[:buflen], buflen)
		}
		if err == nil && n < length {
			err = io.ErrShortBuffer
		}

		return n, err
	}

	if len(m.data) < length {
		m.data = append(m.data, make([]byte, length-len(m.data))...)
	}
	m.data = m.data[:length]

	if length > 0 {
		_, err = io.ReadAtLeast(r, m.data, length)
	}
	return length, err
}
