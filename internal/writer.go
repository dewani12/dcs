//db writes for persistence parallel to publish
//hub run() never blocks on db call

package dcs

import (
	"context"
	"log"
)

const (
	writerQueueSize = 1000
	writerWorkers  = 4
)

type Writer struct{
	queue chan Envelope
	pg *Postgres
}

func NewWriter(pg *Postgres)*Writer{
	w:=&Writer{
		queue:make(chan Envelope,writerQueueSize),
		pg:pg,
	}
	// go w.worker() //can be multiple

	for i := 0; i < writerWorkers; i++ {
		go w.worker()
	}
	return w
}

func (w *Writer)enqueue(env Envelope){
	select{
	case w.queue<-env:
	default:
		log.Println("queue full, dropping messages:",env.FromUserID)
	}
}

func (w *Writer)worker(){
	for env:=range w.queue{
		m:=Message{FromUserID: env.FromUserID,Body: env.Body}
		if env.RoomID!="" {
			m.RoomID = &env.RoomID
		}
		if env.ToUserId!=""{
			m.ToUserId = &env.ToUserId
		}

		if err:=w.pg.saveMessage(context.Background(),m);err!=nil{
			log.Println("save message error:",err)
		}
	}

}
