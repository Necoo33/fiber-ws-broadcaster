## Fiber Wsb — lightweight rooms/broadcasting for Fiber WebSockets

Small, thread-safe, in-memory broadcaster for `github.com/gofiber/contrib/websocket` that helps you:
- Manage rooms and connections
- Broadcast messages to all clients in a room
- Broadcast conditionally (if / if not)
- Add/remove/check connections and rooms safely with locks

The core types are `Broadcaster`, `Room`, and `Connection` (see `wsb.go`).

### Requirements
- Go version as defined in `go.mod` (currently `go 1.25.1`)
- Fiber v2 and Fiber WebSocket contrib

### Quick start
Minimal example showing how to wire a Fiber server with `wsb` and echo chat messages to a room.

```go
package main

import (
    "log"

    "github.com/gofiber/contrib/websocket"
    "github.com/gofiber/fiber/v2"
    "wsb"
)

func main() {
    app := fiber.New()
    broadcaster := wsb.New()

    // WebSocket upgrade gate
    app.Use("/chats", func(c *fiber.Ctx) error {
        if websocket.IsWebSocketUpgrade(c) {
            return c.Next()
        }
        return fiber.ErrUpgradeRequired
    })

    // WebSocket endpoint
    app.Get("/chats", websocket.New(func(conn *websocket.Conn) {
        roomID := conn.Query("room")
        id := conn.Query("id")

        // Ensure room exists, then get it
        br := broadcaster.Handle(roomID)
        room := br.RoomById(roomID)

        // Attach this connection (you can pass any data in the 3rd arg)
        room.Handle(conn, id, nil)

        for {
            messageType, message, err := conn.ReadMessage()
            if err != nil {
                // Clean up this connection
                room.RemoveById(id)
                // Optionally, if you know the room has become empty in your app logic,
                // you can prune it via: br.RemoveById(roomID)
                return
            }

            if messageType == websocket.TextMessage {
                room.Broadcast(message, nil)
            }
        }
    }))

    if err := app.Listen(":3000"); err != nil {
        log.Fatal(err)
    }
}
```

Notes:
- On Chromium-based browsers, the native WebSocket API does not send query params; use Firefox-based browsers to test query-parameter reads like in the example, or pass identifiers via a first message instead.

### API overview

- `type Connection`:
  - Fields: `Id string`, `Data any`, `Conn *websocket.Conn`
  - Methods: `Send([]byte)`, `SendIf([]byte, func(any) bool)`, `SendIfNot([]byte, func(any) bool)`

- `type Room`:
  - Lifecycle: `Add(*Connection)`, `Remove(*Connection)`, `RemoveById(string)`, `RemoveAll()`
  - Lookup/Checks: `ConnectionById(string) *Connection`, `CheckConnectionById(string) bool`
  - Broadcasts: `Broadcast([]byte, any)`, `BroadcastIf([]byte, context.Context, func(*Connection) bool)`, `BroadcastIfNot([]byte, context.Context, func(*Connection) bool)`
  - Helpers: `Handle(*websocket.Conn, id string, data any) *Room` (creates/attaches connection if missing)

- `type Broadcaster`:
  - Rooms: `Add(id string)`, `RoomById(id string) *Room`, `Handle(id string) *Broadcaster`
  - Removal: `Remove(*Room)`, `RemoveById(id string)`, `RemoveAll()`, `RemoveIf(func(*Room) bool)`, `RemoveIfNot(func(*Room) bool)`
  - Checks: `CheckRoom(*Room) bool`, `CheckRoomById(id string) bool`

### Usage pattern
1. Create a broadcaster with `wsb.New()`.
2. For each room you need, call `broadcaster.Handle(roomID)` then `RoomById(roomID)`.
3. For each incoming WebSocket connection, call `room.Handle(conn, id, data)`.
4. Read messages and `room.Broadcast(...)` to echo to everyone in that room.
5. On close/error: `room.RemoveById(id)` and prune empty rooms with `broadcaster.RemoveIf(...)`.

### Testing example
See `wsb_test.go` for a complete runnable example that serves simple HTML pages, upgrades to WebSocket on `/chats`, and broadcasts messages to all clients in the same room.

### Caveats
- This is an in-memory broadcaster; it’s single-process and not distributed.
- You should close connections and prune empty rooms to avoid leaks (see the test example).

### License
MIT


