# socket.io-pm2

[![NPM version](https://badge.fury.io/js/socket.io-pm2.svg)](http://badge.fury.io/js/socket.io-pm2)

## How to use

```js
const io = require('socket.io')(3000);
const pm2Adapter = require('socket.io-pm2');
io.adapter(pm2Adapter());
```

By running socket.io with the `socket.io-pm2` adapter you can run
multiple socket.io instances in different processes on the same pm2 cluster that can
all broadcast and emit events to and from each other.

So any of the following commands:

```js
io.emit('hello', 'to all clients');
io.to('room42').emit('hello', "to all clients in 'room42' room");

io.on('connection', (socket) => {
  socket.broadcast.emit('hello', 'to all clients except sender');
  socket.to('room42').emit('hello', "to all clients in 'room42' room except sender");
});
```

will properly be broadcast to the clients through PM2 IPC (inter-process communication).

If you need to emit events to socket.io instances from a non-socket.io
process, you should use [socket.io-emitter](https://github.com/socketio/socket.io-emitter).

## API

### adapter([opts])

The following options are allowed:

- `key`: the name of the key to pub/sub events on as prefix (`socket.io`)
- `requestsTimeout`: optional, after this timeout the adapter will stop waiting from responses to request (`5000ms`)

### PM2Adapter

The pm2 adapter instances expose the following properties
that a regular `Adapter` does not

- `uid`
- `prefix`
- `requestsTimeout`

### PM2Adapter#clients(rooms:Array, fn:Function)

Returns the list of client IDs connected to `rooms` across all nodes. See [Namespace#clients(fn:Function)](https://github.com/socketio/socket.io#namespaceclientsfnfunction)

```js
io.of('/').adapter.clients((err, clients) => {
  console.log(clients); // an array containing all connected socket ids
});

io.of('/').adapter.clients(['room1', 'room2'], (err, clients) => {
  console.log(clients); // an array containing socket ids in 'room1' and/or 'room2'
});

// you can also use

io.in('room3').clients((err, clients) => {
  console.log(clients); // an array containing socket ids in 'room3'
});
```

### PM2Adapter#clientRooms(id:String, fn:Function)

Returns the list of rooms the client with the given ID has joined (even on another node).

```js
io.of('/').adapter.clientRooms('<my-id>', (err, rooms) => {
  if (err) { /* unknown id */ }
  console.log(rooms); // an array containing every room a given id has joined.
});
```

### PM2Adapter#allRooms(fn:Function)

Returns the list of all rooms.

```js
io.of('/').adapter.allRooms((err, rooms) => {
  console.log(rooms); // an array containing all rooms (accross every node)
});
```

### PM2Adapter#remoteJoin(id:String, room:String, fn:Function)

Makes the socket with the given id join the room. The callback will be called once the socket has joined the room, or with an `err` argument if the socket was not found.

```js
io.of('/').adapter.remoteJoin('<my-id>', 'room1', (err) => {
  if (err) { /* unknown id */ }
  // success
});
```

### PM2Adapter#remoteLeave(id:String, room:String, fn:Function)

Makes the socket with the given id leave the room. The callback will be called once the socket has left the room, or with an `err` argument if the socket was not found.

```js
io.of('/').adapter.remoteLeave('<my-id>', 'room1', (err) => {
  if (err) { /* unknown id */ }
  // success
});
```

### PM2Adapter#remoteDisconnect(id:String, close:Boolean, fn:Function)

Makes the socket with the given id to get disconnected. If `close` is set to true, it also closes the underlying socket. The callback will be called once the socket was disconnected, or with an `err` argument if the socket was not found.

```js
io.of('/').adapter.remoteDisconnect('<my-id>', true, (err) => {
  if (err) { /* unknown id */ }
  // success
});
```

### PM2Adapter#customRequest(data:Object, fn:Function)

Sends a request to every nodes, that will respond through the `customHook` method.

```js
// on every node
io.of('/').adapter.customHook = (data, cb) => {
  cb('hello ' + data);
}

// then
io.of('/').adapter.customRequest('john', function(err, replies){
  console.log(replies); // an array ['hello john', ...] with one element per node
});
```
## Protocol

The `socket.io-pm2` adapter broadcasts and receives messages on particularly named ipc topics.

For global broadcasts the channel name is:
```
prefix + '#' + namespace + '#'
```

In broadcasting to a single room the channel name is:
```
prefix + '#' + namespace + '#' + room + '#'
```


- `prefix`: The base channel name. Default value is `socket.io`. Changed by setting `opts.key` in `adapter(opts)` constructor
- `namespace`: See https://github.com/socketio/socket.io#namespace.
- `room` : Used if targeting a specific room.

A number of other libraries adopt this protocol including:

- [socket.io-emitter](https://github.com/socketio/socket.io-emitter)
- [socket.io-python-emitter](https://github.com/GameXG/socket.io-python-emitter)
- [socket.io-emitter-go](https://github.com/stackcats/socket.io-emitter-go)

## License

MIT
