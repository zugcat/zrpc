package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/zugcat/zrpc/sample/ws"
)

func nope(ctx context.Context, err error) {
	slog.ErrorContext(ctx, err.Error())
	os.Exit(1)
}

func main() {
	ctx := context.Background()

	l, err := net.Listen("tcp", "localhost:")
	if err != nil {
		nope(ctx, err)
	}

	slog.InfoContext(ctx, fmt.Sprintf("listening at http://%v\n", l.Addr()))

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	s := &http.Server{
		Handler:     ws.New(),
		BaseContext: func(net.Listener) context.Context { return ctx },
	}

	errC := make(chan error)
	go func() {
		errC <- s.Serve(l)
	}()

	for err == nil {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "shutting down")
			ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
			s.Shutdown(ctx)
			cancel()
			err = <-errC

		case err = <-errC:
		}
	}

	slog.InfoContext(ctx, "shut down")

	if err != http.ErrServerClosed {
		nope(ctx, err)
	}
}
