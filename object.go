package zrpc

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"weak"

	"github.com/zugcat/zob"
)

type MethodCall struct {
	Name   string // if number is not known
	Number int    // if known
	Args   []any
}

type MethodReturn struct {
	Number int // if Name was used in MethodCall
	Return []any
}

type Object struct {
	id  uint64
	lck *sync.RWMutex // must be able to pass by reference

	// server side
	object  reflect.Value
	numbers []reflect.Value

	// client side
	methods map[string]int
	client  *Client
}

var _ zob.UnmarshalerFrom = &Object{}
var _ zob.MarshalerTo = Object{}

func init() {
	zob.RegisterType[Object]()
	zob.RegisterType[MethodCall]()
	zob.RegisterType[MethodReturn]()
}

// known objects
var nextID atomic.Uint64
var knownObjects sync.Map // [uint64]weak *Object

// NewServerObject creates a new server side proxy object for x.
// x can be nil.
func NewServerObject(x any) *Object {
	id := nextID.Add(1)
	o := &Object{
		id:  id,
		lck: &sync.RWMutex{},
	}
	if x != nil {
		o.object = reflect.ValueOf(x)
	}

	knownObjects.Store(id, weak.Make(o))
	runtime.AddCleanup(o, func(id uint64) { knownObjects.Delete(id) }, id)

	return o
}

// NewClientObject creates a new client side proxy object
func NewClientObject(id uint64, client *Client) *Object {
	return &Object{
		id:      id,
		lck:     &sync.RWMutex{},
		methods: make(map[string]int),
		client:  client,
	}
}

func (o Object) MarshalZOBTo(m zob.Marshaler) error {
	return m.Encode(o.id)
}

func (o *Object) UnmarshalZOBFrom(u zob.Unmarshaler) error {
	return u.Decode(&o.id)
}

// Interface returns the instance behind the server object
func (o *Object) Interface() any {
	if o.object.IsValid() {
		return o.object.Interface()
	}
	return nil
}

// ObjectByID returns an existing server side object
func ObjectByID(id uint64) (*Object, error) {
	po, ok := knownObjects.Load(id)
	if !ok {
		// unknown or long ago released
		return nil, ErrInvalidObject
	}

	o := po.(weak.Pointer[Object]).Value()
	if o == nil {
		// released
		return nil, ErrInvalidObject
	}

	return o, nil
}

func (o *Object) ID() uint64 { return o.id }

// Call is the client side
func (o *Object) Call(method string, args ...any) ([]any, error) {
	o.lck.RLock()
	number, ok := o.methods[method]
	o.lck.RUnlock()
	if ok {
		method = ""
	}

	rch := make(chan any, 1)
	defer close(rch)

	err := o.client.Send(&CallRequest{
		o,
		MethodCall{
			Name:   method,
			Number: number,
			Args:   args,
		},
	}, func(x any) {
		rch <- x
	})

	if err != nil {
		return nil, err
	}

	switch x := (<-rch).(type) {
	case error:
		return nil, x
	case *MethodReturn:
		if x.Number != 0 {
			o.lck.Lock()
			o.methods[method] = x.Number
			o.lck.Unlock()
		}
		return x.Return, nil
	default:
		return nil, fmt.Errorf("%w: received %T", ErrBadResponse, x)
	}
}

// PrepareCall is the top half of the server side
func (o *Object) PrepareCall(c *MethodCall) (reflect.Value, []reflect.Value, error) {
	m, err := o.resolveMethod(c)
	if err != nil {
		return reflect.Value{}, nil, err
	}

	params, err := validateArgs(m.Type(), c.Args)
	if err != nil {
		return m, nil, err
	}

	return m, params, nil
}

// ExecuteCall is the bottom half of the server side
func (o *Object) ExecuteCall(c *MethodCall, m reflect.Value, params []reflect.Value) *MethodReturn {
	var out []reflect.Value

	if m.Type().IsVariadic() {
		out = m.CallSlice(params)
	} else {
		out = m.Call(params)
	}

	mr := &MethodReturn{
		Number: c.Number,
		Return: make([]any, len(out)),
	}

	for n, v := range out {
		if !v.IsValid() {
			continue
		}
		mr.Return[n] = v.Interface()
	}

	return mr
}

func validateArgs(mt reflect.Type, args []any) ([]reflect.Value, error) {
	params := make([]reflect.Value, len(args))
	for n, arg := range args {
		params[n] = reflect.ValueOf(arg)
	}

	ins := mt.NumIn()

	if ins != len(args) {
		return nil, fmt.Errorf("%w: need %d params, got %d",
			ErrBadParams, ins, len(params))
	}

	for n := range ins {
		var err error

		params[n], err = checkOrConvert(params[n], mt.In(n))
		if err != nil {
			return nil, fmt.Errorf("param %d: %w", n, err)
		}
	}

	return params, nil
}

func checkOrConvert(param reflect.Value, in reflect.Type) (reflect.Value, error) {
	if param.Type() == in {
		return param, nil
	}
	if !param.CanConvert(in) {
		return param, fmt.Errorf("%w: %v -> %v", ErrBadParams, param.Type(), in)
	}
	return param.Convert(in), nil
}

func (o *Object) resolveMethod(c *MethodCall) (reflect.Value, error) {
	if c.Number > 0 {
		o.lck.RLock()
		defer o.lck.RUnlock()

		n := c.Number
		c.Number = 0

		if n <= len(o.numbers) {
			return o.numbers[n-1], nil
		}
	} else {
		o.lck.Lock()
		defer o.lck.Unlock()

		if o.object.IsValid() && c.Name != "" {
			m := o.object.MethodByName(c.Name)
			if m.IsValid() {
				o.numbers = append(o.numbers, m)
				c.Number = len(o.numbers)

				return m, nil
			}
		}
	}

	return reflect.Value{}, fmt.Errorf("%w: %s/%d ", ErrNoMethod, c.Name, c.Number)
}
