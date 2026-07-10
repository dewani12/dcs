package dcs

import (
	"context"
	"encoding/json"
	"log"
	"strings"
)

type joinReq struct {
	client *Client
	roomID string
}

type redisMsg struct{
	channel string
	env     Envelope
}

// who's connected - single source of truth
type Hub struct {
	clients      map[*Client]bool
	rooms        map[string]map[*Client]bool
	users 		 map[string]map[*Client]bool
	register     chan *Client
	unregister   chan *Client
	broadcast    chan Envelope
	deliverLocal chan redisMsg
	join         chan joinReq
	leave        chan joinReq
	broker       *Broker
	writer 		 *Writer
}

func (h *Hub)SetBroker(b *Broker){
	h.broker=b
}
func (h *Hub) SetWriter(w *Writer) {
    h.writer=w
}

func NewHub() *Hub {
	return &Hub{
		clients:      make(map[*Client]bool),
		rooms:        make(map[string]map[*Client]bool),
		users:        make(map[string]map[*Client]bool),
		broadcast:    make(chan Envelope),
		deliverLocal: make(chan redisMsg),
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

			if h.users[c.userID] ==nil{
				h.users[c.userID]=make(map[*Client]bool)

				if err := h.broker.subscribe(ctx, "user:"+c.userID); err != nil {
					log.Println("subscribe user error:", err)
				}
			}

			h.users[c.userID][c]=true

		case c := <-h.unregister:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)

				//remove client from every room
				for roomID, clients := range h.rooms {
					if _, i := clients[c]; i {
						delete(clients, c)
						if len(clients) == 0 {
							delete(h.rooms, roomID)
							if err := h.broker.unsubscribe(ctx, "room:"+roomID); err != nil {
								log.Println("unsubscribe room error:", err)
							}
						}
					}
				}
				//remove client from users map
				if clients,ok:=h.users[c.userID];ok{
					delete(clients,c)

					if len(clients)==0{
						delete(h.users,c.userID)

						if err := h.broker.unsubscribe(ctx, "user:"+c.userID); err != nil {
							log.Println("unsubscribe user error",err)
						}
					}
				}

				close(c.send)
			}

		case req := <-h.join:
			if h.rooms[req.roomID] == nil {
				h.rooms[req.roomID] = make(map[*Client]bool)

				if err := h.broker.subscribe(ctx, "room:"+req.roomID); err != nil {
					log.Println("subscribe room error: ", err)
				}
			}
			h.rooms[req.roomID][req.client] = true

		case req := <-h.leave:
			if clients, ok := h.rooms[req.roomID]; ok {
				delete(clients, req.client)
				if len(clients) == 0 {
					delete(h.rooms, req.roomID)

					if err := h.broker.unsubscribe(ctx, "room:"+req.roomID); err != nil {
						log.Println("unsubscribe error:", err)
					}
				}
			}

		case env := <-h.broadcast:
			h.writer.enqueue(env)
			if err := h.broker.publish(ctx, env); err != nil {
				log.Println("publish error: ", err)
			}

		case rm := <-h.deliverLocal:
			payload, err := json.Marshal(rm.env)
			if err != nil {
				continue
			}
			
			var rcp map[*Client]bool
			switch {
			case strings.HasPrefix(rm.channel, "room:"):
				rcp = h.rooms[strings.TrimPrefix(rm.channel, "room:")]
			case strings.HasPrefix(rm.channel, "user:"):
				rcp = h.users[strings.TrimPrefix(rm.channel, "user:")]
			}

			for c:=range rcp{
				select{
				case c.send<-payload:
				default:
					close(c.send)
					delete(h.clients,c)
					delete(rcp,c)	
				}
			}
		}
	}
}
