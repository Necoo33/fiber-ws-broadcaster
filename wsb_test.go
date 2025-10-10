package wsb

import (
	"testing"

	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
)

func TestBroadcaster(t *testing.T) {
	type NameData struct {
		Name string `form:"name" json:"name" xml:"name" db:"name" bson:"name"`
	}

	// fiber instance
	server := fiber.New(fiber.Config{})

	// broadcaster instance
	Broadcaster := New()

	// home page
	server.Get("/", func(c *fiber.Ctx) error {
		c.Set("Content-Type", "text/html; charset=utf-8")

		HomeHtml := `<!DOCTYPE html>
		<html lang='en'>
			<head>
				<meta charset='UTF-8'>
				<meta name='viewport' content='width=device-width, initial-scale=1.0'>
				<title>Document</title>
			</head>
			<body>
				<h1>Hello!</h1>

				<form action='/chat' method='get'>
					<input type='text' name='room' placeholder='room'>
					<input type='text' name='name' placeholder='name'>
					<input type='text' name='id' placeholder='id'>
					<input type='submit' value='send'>
				</form>
			</body>
		</html>`

		return c.SendString(HomeHtml)
	})

	// chat page
	server.Get("/chat", func(c *fiber.Ctx) error {
		c.Set("Content-Type", "text/html; charset=utf-8")

		ChatHtml := `<!DOCTYPE html>
		<html lang='en'>
		<head>
			<meta charset='UTF-8'>
			<meta name='viewport' content='width=device-width, initial-scale=1.0'>
			<title>Document</title>

		</head>
		<body>
			<input type='message' placeholder='send chat' class='message-input'>
			<input type='submit' value='send' class='send-chat-button'>
			<button class='close-button'>close</button>
			
			<div class='chats'>

			</div>

			<style>
				.messages {
					min-width: 100px;
					height: 50px;
					color: white;
					margin: 10px 0;
					border-radius: 20px;
				}

				.my-message {
					background-color: black;
				}

				.other-message {
					background-color: blue;
				}
			</style>

		<script> 
			const chats = document.querySelector('.chats'); 
			const send = document.querySelector('.send-chat-button'); 
			const messageInput = document.querySelector('.message-input');  
			const query = new URLSearchParams(window.location.search); 
			const closeButton = document.querySelector('.close-button');

			/* Note: That configuration doesn't work on chromium based browsers,
			Because they don't let you to send query parameters to websocket 
			routes with Websocket Api. You should try it on firefox based 
			browsers, such as firefox, librewolf etc. */
			
			let websocketUrl = "ws://localhost:3000/chats?name=" + query.get('name') + "&id=" + query.get('id') + "&room=" + query.get('room')

			let websocket = new WebSocket(websocketUrl);

			websocket.addEventListener('open', function() { 
				console.log('WebSocket is open!'); 
			}); 

			websocket.addEventListener('message', async function(event) {
				const message = JSON.parse(event.data); 
				const newParagraph = document.createElement('p'); 
				newParagraph.textContent = message.name + ': ' + message.message; 
				newParagraph.classList.add('messages'); 
				
				if (message.id === query.get('id')) { 
					newParagraph.classList.add('my-message'); 
				} else { 
					newParagraph.classList.add('other-message'); 
				} 
				
				chats.append(newParagraph);
			}); 
			
			websocket.addEventListener('close', function(event) { 
				console.log('WebSocket closed: ', event); 
				console.log('is event bubbled: ', event.bubbles);
				console.log('is event composed: ', event.composed);
				console.log('Code: ' + event.code + ', Reason: ' + event.reason); 
			}); 
			
			websocket.addEventListener('error', function(event) { 
				console.error('WebSocket error: ', event); 
			}); 

			document.addEventListener('beforeunload', function(){
				websocket.close();
			})
			
			send.addEventListener('click', function() { 
				const message = { 
					name: query.get('name'), 
					id: query.get('id'), 
					message: messageInput.value 
				}; 
				
				websocket.send(JSON.stringify(message)); 
				
				messageInput.value = ''; 
			}); 

			closeButton.addEventListener('pointerdown', function() { 
				websocket.close();
			}); 
		</script>
		</body>
		</html>`

		return c.SendString(ChatHtml)
	})

	// websocket upgrade middleware
	server.Use("/chats", func(c *fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) {
			//c.Locals("allowed", true)
			return c.Next()
		}

		return fiber.ErrUpgradeRequired
	})

	// actual websocket handler
	server.Get("/chats", websocket.New(func(c *websocket.Conn) {
		// for testing purposes, we'll get that values from the query parameters.
		// which it'll not work on chromium based browsers, because they don't let you to send query
		// parameters to websocket routes with Websocket Api.
		RoomId := c.Query("room")
		ConnId := c.Query("id")
		Name := c.Query("name")

		// just a data for send to the frontend
		nameData := NameData{
			Name: Name,
		}

		// handle the room with it's id. If it isn't exist, it will be created.
		br := Broadcaster.Handle(RoomId)

		// get the room by id. If it isn't exist, it will be created.
		room := br.RoomById(RoomId)

		// handle the connection with it's id. If it isn't exist, it will be created with the given id and data.
		room.Handle(c, ConnId, nameData)

		for {
			messageType, message, err := c.ReadMessage()

			// if the connection is closed, remove the connection from the room and the broadcaster.
			// sometimes that method returns error that indicates websocket is closed, so also check error case for that.
			if err != nil {
				// remove the connection from the room.
				room.RemoveById(ConnId)

				// remove the room from the broadcaster if it doesn't have any connections.
				br.RemoveIf(func(r *Room) bool {
					return len(r.clients) == 0
				})

				return
			}

			// text message, echo all connections in the room.
			if messageType == 1 {
				room.Broadcast(message, nil)
			}

			// close connection, remove the connection from the room and the broadcaster.
			// this is connection close message, so it's better also handle it.
			if messageType == 8 {
				room.RemoveById(ConnId)

				// remove the room from the broadcaster if it doesn't have any connections.
				br.RemoveIf(func(r *Room) bool {
					return len(r.clients) == 0
				})

				break
			}
		}
	}))

	server.Listen(":3000")
}
