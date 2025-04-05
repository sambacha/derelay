package ports

import (
	"github.com/gorilla/websocket"
)

// WsConnection defines the essential methods needed from a websocket connection
// for the client's read and write pumps.
type WsConnection interface {
	ReadMessage() (messageType int, p []byte, err error)
	WriteMessage(messageType int, data []byte) error
	SetPongHandler(h func(string) error)
	Close() error
	// Add other methods if needed by relay package consumers
}

// Ensure *websocket.Conn satisfies the interface (compile-time check)
var _ WsConnection = (*websocket.Conn)(nil)
