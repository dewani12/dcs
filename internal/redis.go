package dcs

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

var ttl = 30*time.Second

// node connection to redis
type Broker struct {
	rdb    *redis.Client //used for both pubsub and kv(presence)
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

//delivery of message accross nodes
func (b *Broker) publish(ctx context.Context, env Envelope) error {
	payload, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return b.rdb.Publish(ctx, env.Target(), payload).Err()
}

func (b *Broker) subscribe(ctx context.Context, channel string) error {
	return b.pubsub.Subscribe(ctx, channel)
}

func (b *Broker) unsubscribe(ctx context.Context, channel string) error {
	return b.pubsub.Unsubscribe(ctx, channel)
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

		b.hub.deliverLocal <- redisMsg{env.Target(),env}
	}
}

//presence kv for nodes
func (b *Broker)markOnline(ctx context.Context,userID,node string)error{
	return b.rdb.Set(ctx,"presence:"+userID,node,ttl).Err()
}

func (b *Broker)isOnline(ctx context.Context,userID string)(bool,string,error){
	node,err:=b.rdb.Get(ctx,"presence:"+userID).Result()
	if err==redis.Nil{
		return false,"",nil
	}
	if err!=nil{
		return false,"",err
	}
	return true,node,nil
}

// func (b *Broker)clearOnline(ctx context.Context,userID string)error{
// 	return b.rdb.Del(ctx,"presence:"+userID).Err()
// }