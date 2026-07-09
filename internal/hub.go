package dcs

import (
	"context"
	"encoding/json"
	"log"
)

type joinReq struct {
	client *Client
	roomID string
}

// who's connected - single source of truth
type Hub struct {
	clients      map[*Client]bool
	rooms        map[string]map[*Client]bool
	register     chan *Client
	unregister   chan *Client
	broadcast    chan Envelope
	deliverLocal chan Envelope
	join         chan joinReq
	leave        chan joinReq
	broker       *Broker
}

func (h *Hub)SetBroker(b *Broker){
	h.broker=b
}

func NewHub() *Hub {
	return &Hub{
		clients:      make(map[*Client]bool),
		rooms:        make(map[string]map[*Client]bool),
		broadcast:    make(chan Envelope),
		deliverLocal: make(chan Envelope),
		register:     make(chan *Client),
		unregister:   make(chan *Client),
		join:         make(chan joinReq),
		leave:        make(chan joinReq),
	}
}

func (h *Hub) Run() {
	ctx := context.Background()
	for {
		select {
		case c := <-h.register:
			h.clients[c] = true

		case c := <-h.unregister:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)

				//remove client from every room
				for roomID, clients := range h.rooms {
					if _, i := clients[c]; i {
						delete(clients, c)
						if len(clients) == 0 {
							delete(h.rooms, roomID)
							if err := h.broker.unsubscribe(ctx, roomID); err != nil {
								log.Println("unsubscribe error:", err)
							}
						}
					}
				}

				close(c.send)
			}

		case req := <-h.join:
			if h.rooms[req.roomID] == nil {
				h.rooms[req.roomID] = make(map[*Client]bool)

				if err := h.broker.subscribe(ctx, req.roomID); err != nil {
					log.Println("subscribe error: ", err)
				}
			}
			h.rooms[req.roomID][req.client] = true

		case req := <-h.leave:
			if clients, ok := h.rooms[req.roomID]; ok {
				delete(clients, req.client)
				if len(clients) == 0 {
					delete(h.rooms, req.roomID)

					if err := h.broker.unsubscribe(ctx, req.roomID); err != nil {
						log.Println("unsubscribe error:", err)
					}
				}
			}

		case env := <-h.broadcast:
			if err := h.broker.publish(ctx, env); err != nil {
				log.Println("publish error: ", err)
			}

		case env := <-h.deliverLocal:
			payload, err := json.Marshal(env)
			if err != nil {
				continue
			}
			for c := range h.rooms[env.RoomID] {
				select {
				case c.send <- payload:
				//dead connection
				default:
					close(c.send)
					delete(h.clients, c)
					delete(h.rooms[env.RoomID], c)
				}
			}
		}
	}
}
