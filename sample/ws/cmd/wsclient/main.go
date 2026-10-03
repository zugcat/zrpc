package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/coder/websocket"

	"github.com/zugcat/zob"
	"github.com/zugcat/zrpc"
	"github.com/zugcat/zrpc/sample/ws"
)

func nope(err error) {
	fmt.Fprintln(os.Stderr, err.Error())
	os.Exit(1)
}

func main() {
	ctx := context.Background()

	flag.Parse()
	args := flag.Args()

	if len(args) < 3 {
		fmt.Fprintf(os.Stderr, "usage: wsclient url you me\n")
		os.Exit(2)
	}

	c, _, err := websocket.Dial(ctx, args[0], nil)
	if err != nil {
		nope(err)
	}

	conn := websocket.NetConn(ctx, c, websocket.MessageBinary)

	cx := zob.DefaultCodec(conn, conn, zob.AutoFlush(false))
	cli := zrpc.NewClient(cx)

	o, err := cli.Object()
	if err != nil {
		nope(err)
	}

	hello := &ws.HelloStub{Object: o}

	you, me := args[1], args[2]
	str, err := hello.Greet(you, me)

	if err != nil {
		nope(err)
	}

	fmt.Println(str)
}
