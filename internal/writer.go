//db writes for persistence parallel to publish
//hub run() never blocks on db call

package dcs

import (
	"context"
	"log"
)

type Writer struct{
	queue chan Envelope
	pg *Postgres
}

func NewWriter(pg *Postgres,queueSize int)*Writer{
	w:=&Writer{
		queue:make(chan Envelope,queueSize),
		pg:pg,
	}
	go w.worker() //can be multiple
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
