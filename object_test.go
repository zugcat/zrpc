package zrpc_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zugcat/zob"
	"github.com/zugcat/zrpc"
)

type Foo int

func (f Foo) Add(x, y int) int {
	return x + y
}

func (f Foo) Reverse(in ...int) []int {
	slices.Reverse(in)
	return in
}

var errNope = errors.New("nope")

func (f Foo) Error() error {
	return errNope
}

func init() {
	zob.RegisterError(errNope, "github.com/zugcat/zrpc_test.errNope")
}

func TestObject(t *testing.T) {
	o := zrpc.NewServerObject(Foo(0))
	o2, err := zrpc.ObjectByID(o.ID())
	require.NoError(t, err)
	assert.Same(t, o, o2)

	call := func(name string, args ...any) []any {
		mc := &zrpc.MethodCall{Name: name, Args: args}

		m, params, err := o.PrepareCall(mc)
		require.NoError(t, err)

		mr := o.ExecuteCall(mc, m, params)
		assert.NotZero(t, mr.Number)

		mc.Number = mr.Number
		mc.Name = ""

		m, params, err = o.PrepareCall(mc)
		require.NoError(t, err)
		assert.Zero(t, mc.Number)

		return mr.Return
	}

	t.Run("add", func(t *testing.T) {
		r := call("Add", 2, 3)
		assert.Equal(t, 5, r[0])
	})

	t.Run("reverse", func(t *testing.T) {
		r := call("Reverse", []int{1, 2, 3, 4, 5})
		assert.Equal(t, []int{5, 4, 3, 2, 1}, r[0])
	})

	t.Run("Error", func(t *testing.T) {
		r := call("Error")
		assert.ErrorIs(t, r[0].(error), errNope)
	})
}
