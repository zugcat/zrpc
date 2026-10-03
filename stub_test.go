package zrpc_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Fooer is implemented by the Foo object
type Fooer interface {
	// Add adds x and y
	Add(int, int) int
	// Reverse reverses the variadic
	Reverse(...int) []int
	// Error returns errNppe
	Error() error
}

//go:generate go run ./cmd/zrpcgen -out stub_gen_test.go -pkg zrpc_test Fooer .

func TestStub(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	o, stop := serverClient(ctx, t, Foo(0))
	defer stop()

	fs := Fooer(FooerStub{o})

	assert.Equal(t, 5, fs.Add(2, 3))
	assert.Equal(t, []int{5, 4, 3, 2, 1}, fs.Reverse(1, 2, 3, 4, 5))
	assert.ErrorIs(t, fs.Error(), errNope)
}
