# ZRPC

RPC mechanism based on [zob](https://github.com/zugcat/zob).

The basis of the mechanism is the `Object`. As a server, it
accepts a request to call a method of the object it is
wrapping.

As a client, the Object uses its associated `Client`
object to send and receive the results for the requested call.

A `Server` instance is created with a root Object and
codec with an established connection (accepting and managing
connections is the caller's responsibility within the larger
application).

Methods are executed by the server in their own go routines.
The client side interface is synchronous, but is also go
routine safe, and simultaneous calls will run in parallel.

Further Object instances wrapping other callable objects
may be returned as RPC results. This why the server starts with
a so-called "root" instance.

