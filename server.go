package zrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/zugcat/zob"
	"github.com/zugcat/zob/codec"
)

// Server is the server side.
type Server struct {
	cx      codec.Codec
	obj     *Object // root proxy object
	lck     sync.Mutex
	refs    map[uint64]*Object
	replies chan reply
	backlog int
	wg      sync.WaitGroup
	opts    []zob.Option
}

// CallRequest requests a call to an Object's method
type CallRequest struct {
	Object *Object
	Call   MethodCall
}

// ReleaseRequest informs the server the client is done with the vended object
type ReleaseRequest struct {
	Object *Object
}

// ObjectRequest requests the root object.
// This object may be released, but does not need to be.
type ObjectRequest struct{}

func init() {
	zob.RegisterType[CallRequest]()
	zob.RegisterType[ReleaseRequest]()
	zob.RegisterType[ObjectRequest]()
}

type reply struct {
	id  uint64
	rep any
}

// NewServer creates a new server vending from the root object x.
func NewServer(cx codec.Codec, x any, backlog int, opts ...zob.Option) *Server {
	return &Server{
		cx:      cx,
		obj:     NewServerObject(x),
		refs:    make(map[uint64]*Object),
		backlog: backlog,
		opts:    opts,
	}
}

// Run runs the server until an error occurs.
// (such as the input to the codec closing).
// The context will also be polled for cancelation,
// however codec reads block.
func (s *Server) Run(ctx context.Context) error {
	s.replies = make(chan reply)
	limiter := make(chan struct{}, s.backlog)
	defer close(limiter)

	group, ctx := errgroup.WithContext(ctx)

	m := zob.NewMarshaler(s.cx, s.opts...)

	group.Go(func() error {
		var rep reply

		for {
			select {
			case rep = <-s.replies:
			case <-ctx.Done():
				return nil
			}

			if err := m.Marshal(rep.id); err != nil {
				return err
			}
			if err := m.Marshal(rep.rep); err != nil {
				return err
			}
			if err := m.Flush(); err != nil {
				return err
			}

			<-limiter
		}
	})

	u := zob.NewUnmarshaler(s.cx, s.opts...)

	group.Go(func() error {
		for {
			var id uint64
			var req any

			select {
			case limiter <- struct{}{}:
			case <-ctx.Done():
				return nil
			}

			if err := u.Unmarshal(&id); err != nil {
				return err
			}
			if err := u.Unmarshal(&req); err != nil {
				return err
			}

			if err := s.dispatch(id, req); err != nil {
				s.reply(id, err)
			}
		}
	})

	err := group.Wait()

	if err != nil && !errors.Is(err, io.EOF) {
		slog.ErrorContext(ctx, fmt.Sprintf("zrpc server: %v", err))
	}

	s.wg.Wait()
	close(s.replies)

	return err
}

func (s *Server) reply(id uint64, rep any) {
	s.replies <- reply{id: id, rep: rep}
}

func (s *Server) dispatch(id uint64, req any) error {
	switch req := req.(type) {
	case *CallRequest:
		obj, err := ObjectByID(req.Object.ID())
		if err != nil {
			return err
		}
		return s.call(id, obj, &req.Call)

	case *ReleaseRequest:
		obj, err := ObjectByID(req.Object.ID())
		if err != nil {
			return err
		}

		s.lck.Lock()
		delete(s.refs, obj.ID())
		s.lck.Unlock()

		s.reply(id, nil)
		return nil

	case *ObjectRequest:
		s.reply(id, s.obj)
		return nil
	}

	return fmt.Errorf("%w: %T", ErrBadRequest, req)
}

func (s *Server) call(id uint64, obj *Object, mc *MethodCall) error {
	m, params, err := obj.PrepareCall(mc)
	if err != nil {
		return err
	}

	s.wg.Go(func() {
		mr := obj.ExecuteCall(mc, m, params)
		s.reply(id, mr)
	})

	return nil
}

// NewObject creates a new server side proxy object and
// keeps it referenced.
func (s *Server) NewObject(x any) *Object {
	o := NewServerObject(x)

	s.lck.Lock()
	s.refs[o.ID()] = o
	s.lck.Unlock()

	return o
}
