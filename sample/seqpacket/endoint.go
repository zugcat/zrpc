package seqpacket

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"unsafe"
)

var (
	MaxBacklog    = int64(1024 * 1024)
	MaxSendBuffer = 0x8_0000
)

var (
	ErrNoBuffer = errors.New("invalid buffer tag")
	ErrProtocol = errors.New("protocol error")
	ErrBounds   = errors.New("buffer overrun")
	ErrInUse    = errors.New("tag in use")
)

const bufsPerEndpoint = 4

// Addr implements [net.Addr] and indicates virtual endpoints
// used by the [PacketConn]
type Addr uint64

func (a *Addr) bytes() []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(a)), 8)
}

func (a Addr) Network() string { return "seqpacket" }
func (a Addr) String() string {
	ui := binary.BigEndian.Uint64(a.bytes())
	return strconv.FormatUint(ui, 10)
}

// Resolve creates an [Addr], parseable as per [Addr.String]
func Resolve(s string) (a Addr, err error) {
	ui, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, err
	}
	binary.BigEndian.PutUint64(a.bytes(), ui)
	return a, nil
}

// Endpoint terminates a lower layer connection to
// create [PacketConn] instances
type Endpoint struct {
	rd io.ReadCloser
	wr io.Writer

	rlck             sync.Mutex
	wlck             sync.Mutex
	cond             *sync.Cond
	inputs           map[Addr][]*message
	reading, rclosed bool
	wclosed          bool
	msgpool          *sync.Pool
	backlog          int64
	seq              uint64
	ackSeq           uint64
	bufs             map[Addr]*Buffer
}

// NewEndpoint creates a new endpoint from rd and wr
func NewEndpoint(rd io.ReadCloser, wr io.Writer) *Endpoint {
	e := &Endpoint{
		rd:     rd,
		wr:     wr,
		inputs: make(map[Addr][]*message),
		msgpool: &sync.Pool{
			New: func() any {
				return &message{}
			},
		},
		bufs: make(map[Addr]*Buffer),
	}
	e.cond = sync.NewCond(&e.rlck)
	return e
}

// Listen creates a [PacketConn]
func (e *Endpoint) Listen(src Addr) *PacketConn {
	return newListener(e, src)
}

// Dial creates a [PacketConn] with a destination
func (e *Endpoint) Dial(src, dst Addr) *PacketConn {
	p := newListener(e, src)
	p.dst = &dst
	return p
}

// Close closes the endpoint
func (e *Endpoint) Close() error {
	e.rlck.Lock()
	e.rclosed = true
	err := e.rd.Close()
	e.rlck.Unlock()

	e.wlck.Lock()
	e.wclosed = true
	e.wlck.Unlock()

	return err
}

// Buffer registers a server side buffer
func (e *Endpoint) Buffer(tag Addr, capacity int64) (buf *Buffer, err error) {
	e.rlck.Lock()
	defer e.rlck.Unlock()

	if _, ok := e.bufs[tag]; ok {
		return nil, ErrInUse
	}

	for !e.rclosed && len(e.bufs) >= bufsPerEndpoint {
		e.cond.Wait()
	}

	if e.rclosed {
		return nil, net.ErrClosed
	}

	buf, err = newBuffer(tag, capacity)
	if err != nil {
		return nil, err
	}

	e.bufs[tag] = buf
	return buf, nil
}

// Invalidate unregisters a server side buffer
func (e *Endpoint) Invalidate(buf *Buffer) error {
	e.rlck.Lock()
	defer e.rlck.Unlock()

	if _, ok := e.bufs[buf.Tag()]; !ok {
		return ErrNoBuffer
	}

	e.invalidate(buf)
	return nil
}

func (e *Endpoint) invalidate(buf *Buffer) {
	delete(e.bufs, buf.Tag())
	e.cond.Broadcast()
}

// readMsg reads a message desined for dst.
// p opportunistically avoids a copy if the message was read directly.
// if polling is true, returns when there are no outstanding acks or an ack has been received.
// readMsg returns in all cases of the endpoint is closed.
func (e *Endpoint) readMsg(p []byte, dst Addr, polling bool) (int, *message, error) {
	e.rlck.Lock()
	defer e.rlck.Unlock()

	for {
		if e.rclosed {
			return 0, nil, net.ErrClosed
		}
		if polling && e.seq == e.ackSeq {
			return 0, nil, nil
		}

		if input := e.inputs[dst]; !polling && len(input) > 0 {
			msg := input[0]

			input = input[1:]
			e.inputs[dst] = input

			newBacklog := e.backlog - int64(len(msg.data))
			if e.backlog >= MaxBacklog && newBacklog < MaxBacklog {
				e.cond.Signal()
			}
			e.backlog = newBacklog

			return len(msg.data), msg, nil
		}

		if e.reading || e.backlog >= MaxBacklog {
			// somebody else is reader, or we are backlogged
			e.cond.Wait()
		} else {
			// our turn to become reader
			e.reading = true
			e.rlck.Unlock()

			msg := e.msgpool.Get().(*message)
			n, err := msg.read(e.rd, dst, p)

			e.rlck.Lock()
			e.reading = false

			switch msg.msgtype {
			case readMsg:
				buf, ok := e.bufs[msg.dst]
				if !ok {
					return 0, nil, ErrNoBuffer
				}

				buf.SetOffset(int64(msg.soff))

				length := int(min(int64(msg.len), buf.len-buf.off))
				buf.seg = length

				msg.src, msg.dst = msg.dst, msg.src
				msg.len = uint32(length)
				msg.msgtype = sendMsg

				_, err = e.write(msg, buf)

				if (err != nil) || (msg.flags&invalidateFlag) != 0 {
					e.invalidate(buf)
				}

				if err != nil {
					return 0, msg, err
				}

			case sendMsg:
				buf, ok := e.bufs[msg.dst]
				if !ok {
					return 0, nil, ErrNoBuffer
				}

				buf.SetOffset(int64(msg.doff))
				buf.seg = int(msg.len)

				_, err = io.Copy(buf, e.rd)
				e.ackSeq = msg.seq

				if err != nil {
					return 0, msg, err
				}

			default:
				if !polling && msg.dst == dst && (msg.msgtype <= closeMsg) {
					e.cond.Broadcast()
					return n, msg, err
				}

				e.backlog += int64(len(msg.data))
				e.inputs[msg.dst] = append(e.inputs[msg.dst], msg)
			}

			e.cond.Broadcast()
		}
	}
}

func (e *Endpoint) readFrom(p []byte, dst Addr) (n int, src net.Addr, err error) {
	n, msg, err := e.readMsg(p, dst, false)
	if msg != nil {
		defer e.msgpool.Put(msg)

		if msg.msgtype == closeMsg {
			return 0, msg.src, io.EOF
		}
	} else {
		return 0, nil, err
	}

	if len(msg.data) > 0 {
		n := copy(p, msg.data)
		if n < int(msg.len) {
			err = io.ErrShortBuffer
		}
		return n, msg.src, err
	}

	return n, msg.src, err
}

func (e *Endpoint) close(src, dst Addr) error {
	_, err := e.write(&message{
		src:     src,
		dst:     dst,
		msgtype: closeMsg,
	}, nil)

	return err
}

func (e *Endpoint) writeTo(p []byte, src, dst Addr) (int, error) {
	return e.write(&message{
		src:     src,
		dst:     dst,
		len:     uint32(len(p)),
		msgtype: normalMsg,
		data:    p,
	}, nil)
}

// ReadBuffer sends a read request to dst at remote offset off.
// The result is sent to buf at its local offset.
// If invalidate is true, remote buffer is released once completed.
func (e *Endpoint) ReadBuffer(buf *Buffer, dst Addr, off int64, length int, invalidate bool) error {
	var flags uint8

	var seq uint64
	doff := buf.off

	for o := 0; o < length; o += MaxSendBuffer {
		if invalidate && (length-o) <= MaxSendBuffer {
			flags |= invalidateFlag
		}

		msg := &message{
			msgtype: readMsg,
			src:     buf.tag,
			dst:     dst,
			soff:    uint64(off + int64(o)),
			doff:    uint64(doff + int64(o)),
			len:     uint32(min(length, MaxSendBuffer)),
			flags:   flags,
		}

		// this method of controlling overlap can starve other simulatanous
		// go routines using ReadBuffer on other buffers.
		if err := e.waitAck(seq - 2); err != nil {
			return err
		}

		if _, err := e.write(msg, nil); err != nil {
			return err
		}

		seq = msg.seq
	}

	if err := e.waitAck(seq); err != nil {
		return err
	}
	buf.seg = 0

	return nil
}

func (e *Endpoint) waitAck(seq uint64) error {
	for {
		_, msg, err := e.readMsg(nil, ^Addr(0), true)
		if err != nil {
			return err
		}
		if msg == nil || int64(msg.seq-seq) > 0 {
			return nil
		}
	}
}

func (e *Endpoint) write(msg *message, buf *Buffer) (int, error) {
	e.wlck.Lock()
	defer e.wlck.Unlock()

	if e.wclosed {
		return 0, net.ErrClosed
	}

	if msg.msgtype == readMsg {
		e.seq++
		msg.seq = e.seq
	}

	n, err := msg.write(e.wr, buf)
	if err != nil {
		e.wclosed = true
	}

	return n, err
}
