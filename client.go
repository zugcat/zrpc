package zrpc

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/zugcat/zob"
	"github.com/zugcat/zob/codec"
)

// Client is the client side
type Client struct {
	lck      sync.Mutex
	m        zob.Marshaler
	requests sync.Map // uint64:func(any)
	err      atomic.Value
	done     chan struct{}
	nextID   atomic.Uint64
}

// NewClient creates a new Client
func NewClient(cx codec.Codec, opts ...zob.Option) *Client {
	c := &Client{
		m:    zob.NewMarshaler(cx, opts...),
		done: make(chan struct{}),
	}

	d := zob.NewUnmarshaler(cx, opts...)
	go func() {
		var err error

		for err == nil {
			var id uint64
			var rep any

			err = d.Unmarshal(&id)
			if err == nil {
				err = zob.UnmarshalFrom(d, &rep, zob.WithUnmarshalers(
					zob.UnmarshalFromFunc(func(u zob.Unmarshaler, o *Object) error {
						if err := o.UnmarshalZOBFrom(u); err != nil {
							return err
						}
						*o = *NewClientObject(o.ID(), c)
						return nil
					}),
				))
			}

			if err != nil {
				c.CloseError(err)
				break
			}

			if req, ok := c.requests.LoadAndDelete(id); ok {
				req := req.(func(any))
				req(rep)
			}

			err, _ = c.err.Load().(error)
		}

		c.requests.Range(func(key any, value any) bool {
			value.(func(any))(err)
			return true
		})
	}()

	return c
}

func (c *Client) Wait() error {
	<-c.done
	return c.err.Load().(error)
}

func (c *Client) Close() error {
	return c.CloseError(nil)
}

func (c *Client) CloseError(err error) error {
	if err == nil {
		err = ErrClosed
	}

	if !c.err.CompareAndSwap(nil, err) {
		return ErrClosed
	}

	close(c.done)
	return nil
}

func (c *Client) Send(req any, done func(any)) error {
	if err, _ := c.err.Load().(error); err != nil {
		return err
	}

	id := c.nextID.Add(1)
	c.requests.Store(id, done)

	c.lck.Lock()
	err := c.m.Marshal(id)
	if err == nil {
		err = c.m.Marshal(req)
	}
	if err == nil {
		err = c.m.Flush()
	}
	c.lck.Unlock()

	if err != nil {
		c.requests.Delete(id)
		c.CloseError(err)
		return err
	}

	// check again for client error state. If we're now in error, we could
	// race the reader shutdown, so if completion hasn't yet been called,
	// make sure it won't and return an error here. Otherwise the completion
	// will report the error.
	if err, _ := c.err.Load().(error); err != nil {
		if _, ok := c.requests.LoadAndDelete(id); ok {
			return err
		}
	}

	return nil
}

// Object requests the root object
func (c *Client) Object() (*Object, error) {
	ch := make(chan any, 1)
	if err := c.Send(&ObjectRequest{}, func(res any) {
		ch <- res
	}); err != nil {
		return nil, err
	}
	switch x := (<-ch).(type) {
	case *Object:
		return x, nil
	case error:
		return nil, x
	default:
		return nil, fmt.Errorf("%w: type %T", ErrBadResponse, x)
	}
}
