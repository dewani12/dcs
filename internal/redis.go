package dcs

import (
	"context"
	"encoding/json"
	"log"

	"github.com/redis/go-redis/v9"
)

// node connection to redis
type Broker struct {
	rdb    *redis.Client
	pubsub *redis.PubSub
	hub    *Hub
}

func NewBroker(addr string, hub *Hub) *Broker {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	//dynamically subscribes to channel
	ps := rdb.Subscribe(context.Background())

	return &Broker{
		rdb:    rdb,
		pubsub: ps,
		hub:    hub,
	}
}

func (b *Broker) publish(ctx context.Context, env Envelope) error {
	payload, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return b.rdb.Publish(ctx, "room:"+env.RoomID, payload).Err()
}

func (b *Broker) subscribe(ctx context.Context, roomID string) error {
	return b.pubsub.Subscribe(ctx, "room:"+roomID)
}

func (b *Broker) unsubscribe(ctx context.Context, roomID string) error {
	return b.pubsub.Unsubscribe(ctx, "room:"+roomID)
}

// redis handover msgs to hub for local delivery
func (b *Broker) Run() {
	ch := b.pubsub.Channel()

	for msg := range ch {
		var env Envelope

		if err := json.Unmarshal([]byte(msg.Payload), &env); err != nil {
			log.Println("bad envelope from redis: ", err)
			continue
		}

		b.hub.deliverLocal <- env
	}
}
