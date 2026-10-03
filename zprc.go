// Package zrpc provides an rpc service on top of zob
package zrpc

import (
	"errors"

	"github.com/zugcat/zob"
)

var (
	ErrNoMethod      = errors.New("no method")
	ErrBadParams     = errors.New("bad parameters")
	ErrClosed        = errors.New("closed")
	ErrInvalidObject = errors.New("invalid object")
	ErrBadRequest    = errors.New("bad request")
	ErrBadResponse   = errors.New("bad response")
)

func init() {
	zob.RegisterError(ErrNoMethod, "github.com/zugcat/zrpc.ErrNoMethod")
	zob.RegisterError(ErrBadParams, "github.com/zugcat/zrpc.ErrBadParams")
	zob.RegisterError(ErrInvalidObject, "github.com/zugcat/zrpc.ErrInvalidObject")
	zob.RegisterError(ErrBadRequest, "github.com/zugcat/zrpc.ErrBadRequest")
}
