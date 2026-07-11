package dcs

import (
	"context"
	"log"
	"net/http"
	"fmt"

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

func ServePresence(b *Broker,w http.ResponseWriter, r *http.Request){
	userId:=r.URL.Query().Get("user")
	online,node,err:=b.isOnline(context.Background(),userId);

	if err!=nil{
		http.Error(w,"error",http.StatusInternalServerError)
		return
	}	
		
	fmt.Fprintf(w, `{"user":%q,"online":%v,"node":%q}`, userId, online, node)
}
