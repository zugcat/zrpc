package seqpacket

import (
	"errors"
	"net"
	"time"
)

var (
	ErrNotConn = errors.New("not connected")
	ErrAddr    = errors.New("bad address type")
)

// PacketConn is a [net.PacketConn] implementation using [Endpoint]
type PacketConn struct {
	e   *Endpoint
	src Addr
	dst *Addr
}

var _ net.PacketConn = (*PacketConn)(nil)

func newListener(e *Endpoint, src Addr) *PacketConn {
	return &PacketConn{
		e:   e,
		src: src,
	}
}

func (p *PacketConn) Endpoint() *Endpoint {
	return p.e
}

func (p *PacketConn) ReadFrom(buf []byte) (int, net.Addr, error) {
	return p.e.readFrom(buf, p.src)
}

func (p *PacketConn) WriteTo(buf []byte, addr net.Addr) (int, error) {
	var dst Addr

	if addr == nil {
		if p.dst == nil {
			return 0, ErrNotConn
		}
		dst = *p.dst
	} else {
		var ok bool
		if dst, ok = addr.(Addr); !ok {
			return 0, ErrAddr
		}
	}

	return p.e.writeTo(buf, p.src, dst)
}

func (p *PacketConn) Read(buf []byte) (int, error) {
	n, _, err := p.ReadFrom(buf)
	return n, err
}

func (p *PacketConn) Write(buf []byte) (int, error) {
	return p.WriteTo(buf, nil)
}

// Close sends a close message to the peer, if connected
func (p *PacketConn) Close() error {
	if p.dst == nil {
		return ErrNotConn
	}
	return p.e.close(p.src, *p.dst)
}

func (p *PacketConn) LocalAddr() net.Addr                { return p.src }
func (p *PacketConn) SetDeadline(t time.Time) error      { return nil }
func (p *PacketConn) SetReadDeadline(t time.Time) error  { return nil }
func (p *PacketConn) SetWriteDeadline(t time.Time) error { return nil }
