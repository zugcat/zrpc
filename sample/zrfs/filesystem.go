package zrfs

import (
	"context"
	"io"
	"io/fs"

	"github.com/zugcat/zob"
	"github.com/zugcat/zob/codec"
	"github.com/zugcat/zrpc"
	"github.com/zugcat/zrpc/sample/seqpacket"
)

type Connection struct {
	e *seqpacket.Endpoint
	s *zrpc.Server
}

func New(rd io.ReadCloser, wr io.Writer, fsys fs.FS) *Connection {
	e := seqpacket.NewEndpoint(rd, wr)
	es := e.Dial(0, 0)
	cx := zob.DefaultCodec(es, es, zob.AutoFlush(false))

	codec.RegisterType[fs.FileMode](cx)
	codec.RegisterType[fs.PathError](cx)
	codec.RegisterType[seqpacket.Addr](cx)

	s := zrpc.NewServer(cx, newRootHandle(es, fsys), 100,
		zob.WithMarshalers(
			zob.MarshalToFunc(func(m zob.Marshaler, h HandleObject) error {
				return m.Encode(h.Handle.(*handle).Object.ID())
			}),
		),
		zob.WithUnmarshalers(
			zob.UnmarshalFromFunc(func(u zob.Unmarshaler, h *HandleObject) error {
				var id uint64
				if err := u.Decode(&id); err != nil {
					return err
				}
				o, err := zrpc.ObjectByID(id)
				if err != nil {
					return err
				}
				h.Handle = o.Interface().(*handle)
				return nil
			}),
		),
	)

	return &Connection{e: e, s: s}
}

func (c *Connection) Run(ctx context.Context) error {
	return c.s.Run(ctx)
}

func (c *Connection) Close() error {
	return c.e.Close()
}
