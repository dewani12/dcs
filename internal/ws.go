package dcs

import (
	"log"
	"net/http"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize: 1024,
	WriteBufferSize: 1024,
	//restrict to known origins
	CheckOrigin: func(r *http.Request) bool {return true},
}

func ServeWS(hub *Hub,w http.ResponseWriter,r *http.Request){
	userID:=r.URL.Query().Get("user")

	conn,err:=upgrader.Upgrade(w,r,nil)
	if err!=nil{
		log.Println("upgrade error:", err)
		return
	}
	log.Println("client connected:",userID)

	client:=&Client{
		hub:hub,
		conn:conn,
		send: make(chan []byte,256),
		userID: userID,
	}

	hub.register<-client

	go client.writePump()
	go client.readPump()
}
