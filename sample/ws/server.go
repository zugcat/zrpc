package ws

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/coder/websocket"

	"github.com/zugcat/zob"
	"github.com/zugcat/zrpc"
)

type Hello interface {
	Greet(you, me string) (string, error)
}

//go:generate go run ../../cmd/zrpcgen -out hello_gen.go Hello .

type hello struct{}

var _ Hello = hello{}

func (h hello) Greet(you, me string) (string, error) {
	return fmt.Sprintf("Hello, %s, I am %s.", me, you), nil
}

type HelloHandler struct {
	h Hello
}

func New() *HelloHandler {
	return &HelloHandler{
		h: hello{},
	}
}

func (h HelloHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "%v\r\n", err)
		return
	}
	defer c.Close(websocket.StatusNormalClosure, "bye")

	ctx = context.WithoutCancel(ctx)
	conn := websocket.NetConn(ctx, c, websocket.MessageBinary)

	cx := zob.DefaultCodec(conn, conn, zob.AutoFlush(false))
	s := zrpc.NewServer(cx, h.h, 10)
	errC := make(chan error)
	go func() { errC <- s.Run(ctx) }()

	err = <-errC
	if !errors.Is(err, io.EOF) {
		slog.ErrorContext(ctx, err.Error())
	} else {
		slog.InfoContext(ctx, "done!")
	}
}
