package wsb

import (
	"context"
	"errors"
	"sync"

	"github.com/gofiber/contrib/websocket"
)

// Connection is a websocket connection
type Connection struct {
	Id   string          `form:"id" json:"id" xml:"id" db:"id" bson:"id"`
	Data any             `form:"data" json:"data" xml:"data" db:"data" bson:"id"`
	Conn *websocket.Conn `form:"conn" json:"conn" xml:"conn" db:"conn" bson:"id"`
}

// Room is a collection of connections
type Room struct {
	Id      string `form:"id" json:"id" xml:"id" db:"id" bson:"id"`
	clients map[*Connection]bool
	mu      sync.RWMutex
}

// Broadcaster is a collection of rooms
type Broadcaster struct {
	Rooms map[string]*Room
	mu    sync.RWMutex
}

// Constructors

// creates a new broadcaster
func New() Broadcaster {
	return Broadcaster{
		Rooms: make(map[string]*Room),
	}
}

// creates a new room
func NewRoom(id string) Room {
	return Room{
		Id:      id,
		clients: make(map[*Connection]bool),
	}
}

// creates a new connection
func CreateConnection(conn *websocket.Conn, id string, data any) *Connection {
	return &Connection{
		Id:   id,
		Data: data,
		Conn: conn,
	}
}

// Connection Methods

// sends a message to the connection
func (c *Connection) Send(data []byte) error {
	if c.Conn == nil {
		return errors.New("connection is nil")
	}

	return c.Conn.WriteMessage(1, data)
}

// sends a message to the connection if the condition is met
func (c *Connection) SendIf(data []byte, cond func(any) bool) error {
	if cond(c.Data) {
		return nil
	}

	if c.Conn == nil {
		return errors.New("connection is nil")
	}

	return c.Conn.WriteMessage(1, data)
}

// sends a message to the connection if the condition is not met
func (c *Connection) SendIfNot(data []byte, cond func(any) bool) error {
	if !cond(c.Data) {
		return nil
	}

	if c.Conn == nil {
		return errors.New("connection is nil")
	}

	return c.Conn.WriteMessage(1, data)
}

// Room Methods

// adds a connection to the room
func (r *Room) Add(c *Connection) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.clients[c] = true
}

// removes a connection from the room
func (r *Room) Remove(c *Connection) {
	r.mu.Lock()
	defer r.mu.Unlock()

	c.Conn.Close()
	delete(r.clients, c)
}

// handles a connection with it's id. If it isn't exist, it will be created with the given id and data and return the room, otherwise it will return the room.
func (r *Room) Handle(c *websocket.Conn, id string, data any) *Room {
	r.mu.Lock()
	defer r.mu.Unlock()

	i := 0

	for c := range r.clients {
		if c.Id == id {
			i++
		}
	}

	if i == 0 {
		conn := CreateConnection(c, id, data)
		r.clients[conn] = true
	}

	return r
}

// returns a connection by id
func (r *Room) ConnectionById(id string) *Connection {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for c := range r.clients {
		if c.Id == id {
			return c
		}
	}

	return nil
}

// removes a connection from the room by id
func (r *Room) RemoveById(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for c := range r.clients {
		if c.Id == id {
			c.Conn.Close()
			delete(r.clients, c)
		}
	}
}

// removes all connections from the room
func (r *Room) RemoveAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for c := range r.clients {
		c.Conn.Close()
	}

	r.clients = make(map[*Connection]bool)
}

// removes a connection from the room if the condition is met
func (r *Room) RemoveIf(cond func(*Connection) bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if cond != nil {
		for c := range r.clients {
			if cond(c) {
				c.Conn.Close()
				delete(r.clients, c)
			}
		}
	}
}

// removes a connection from the room if the condition is not met
func (r *Room) RemoveIfNot(cond func(*Connection) bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for c := range r.clients {
		if cond(c) {
			c.Conn.Close()
			delete(r.clients, c)
		}
	}
}

func (r *Room) IsRoomEmpty() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.clients) == 0
}

// checks if a connection exists in the room by id
func (r *Room) CheckConnectionById(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for c := range r.clients {
		if c.Id == id {
			return true
		}
	}

	return false
}

// broadcasts a message to the room
func (r *Room) Broadcast(data []byte, ctx any) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ctx, cancel := resolveContext(ctx)

	if cancel != nil {
		defer cancel()
	}

	var wg sync.WaitGroup

	for c := range r.clients {
		wg.Add(1)

		go func(c *Connection) {
			defer wg.Done()

			select {
			case <-ctx.(context.Context).Done():
				return
			default:
				c.Conn.WriteMessage(websocket.TextMessage, data)
			}
		}(c)
	}

	wg.Wait()
}

// broadcasts a message to the room if the condition is met
func (r *Room) BroadcastIf(data []byte, ctx any, cond func(*Connection) bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ctx, cancel := resolveContext(ctx)

	if cancel != nil {
		defer cancel()
	}

	var wg sync.WaitGroup

	for c := range r.clients {
		if !cond(c) {
			continue
		}

		wg.Add(1)

		go func(c *Connection) {
			defer wg.Done()

			select {
			case <-ctx.(context.Context).Done():
				return
			default:
				c.Conn.WriteMessage(websocket.TextMessage, data)
			}
		}(c)
	}

	wg.Wait()
}

// broadcasts a message to the room if the condition is not met
func (r *Room) BroadcastIfNot(data []byte, ctx any, cond func(*Connection) bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ctx, cancel := resolveContext(ctx)

	if cancel != nil {
		defer cancel()
	}

	var wg sync.WaitGroup

	for c := range r.clients {
		if cond(c) {
			continue
		}

		wg.Add(1)

		go func(c *Connection) {
			defer wg.Done()

			select {
			case <-ctx.(context.Context).Done():
				return
			default:
				c.Conn.WriteMessage(1, data)
			}
		}(c)
	}

	wg.Wait()
}

// Broadcaster Methods

// adds a room to the broadcaster
func (b *Broadcaster) Add(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.Rooms[id]; !exists {
		b.Rooms[id] = &Room{
			Id:      id,
			clients: make(map[*Connection]bool),
		}
	}
}

// returns a room by it's id or nil if it doesn't exist
func (b *Broadcaster) RoomById(id string) *Room {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for r := range b.Rooms {
		if r == id {
			return b.Rooms[r]
		}
	}

	return nil
}

// handles a room with it's id. If it isn't exist, it will be created, otherwise it'll do nothing.
func (b *Broadcaster) Handle(id string) *Broadcaster {
	room := b.RoomById(id)

	if room == nil {
		b.Add(id)
	}

	return b
}

// removes a room from the broadcaster, and closes all connections from the room
func (b *Broadcaster) Remove(r *Room) {
	b.mu.Lock()
	defer b.mu.Unlock()

	r.RemoveAll()
	delete(b.Rooms, r.Id)
}

// checks if a room exists in the broadcaster
func (b *Broadcaster) CheckRoom(r *Room) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.Rooms[r.Id] != nil
}

// checks if a room exists in the broadcaster by id
func (b *Broadcaster) CheckRoomById(id string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for r := range b.Rooms {
		if r == id {
			return true
		}
	}

	return false
}

// removes a room from the broadcaster, and closes all connections from the room by id
func (b *Broadcaster) RemoveById(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for r := range b.Rooms {
		if r == id {
			b.Rooms[r].RemoveAll()
			delete(b.Rooms, r)
		}
	}
}

// removes all rooms from the broadcaster, and closes all connections from the rooms
func (b *Broadcaster) RemoveAll() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for r := range b.Rooms {
		b.Rooms[r].RemoveAll()
		delete(b.Rooms, r)
	}
}

// removes a room from the broadcaster if condition is met, and closes all connections of it
func (b *Broadcaster) RemoveIf(cond func(*Room) bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for r := range b.Rooms {
		if cond(b.Rooms[r]) {
			b.Rooms[r].RemoveAll()
			delete(b.Rooms, r)
		}
	}
}

// removes a room from the broadcaster if condition is not met, and closes all connections of it
func (b *Broadcaster) RemoveIfNot(cond func(*Room) bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for r := range b.Rooms {
		if !cond(b.Rooms[r]) {
			b.Rooms[r].RemoveAll()
			delete(b.Rooms, r)
		}
	}
}

// Helpers

// resolves the context from the any type
func resolveContext(ctx any) (context.Context, context.CancelFunc) {
	switch v := ctx.(type) {
	case context.Context:
		return v, nil
	case nil:
		return context.WithCancel(context.Background())
	default:
		return context.WithCancel(context.Background())
	}
}
