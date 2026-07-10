package dcs

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

const (
	pongWait   = 60 * time.Second
	pingPeriod = 55 * time.Second
	writeWait  = 10 * time.Second
)

type Client struct {
	hub  *Hub
	conn *websocket.Conn
	//buffered channel for outbound msgs
	send chan []byte
	userID string 
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	//dead connection detector
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			log.Println("readPump closing:", err)
			break
		}

		log.Println("RAW: ", string(raw))

		var env Envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			log.Println("bad envelope ", err)
			continue
		}
		switch env.Type {
		case "join":
			c.hub.join <- joinReq{client: c, roomID: env.RoomID}
		case "leave":
			c.hub.leave <- joinReq{client: c, roomID: env.RoomID}
		case "msg":
			c.hub.broadcast <- env
		}
	}
}

func (c *Client) writePump() {
	t := time.NewTicker(pingPeriod)
	defer func() {
		t.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}

		case <-t.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
