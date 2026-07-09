package main

import (
	"log"
	"flag"
	"net/http"

	dcs "github.com/dewani12/dcs/internal"
)

func main(){
	addr:=flag.String("addr",":5000","http service address")
	redisAddr:=flag.String("redis", "localhost:6379", "redis address")
	flag.Parse()

	hub:=dcs.NewHub()
	b:= dcs.NewBroker(*redisAddr,hub)
	
	hub.SetBroker(b)

	go b.Run()
	go hub.Run()

	http.HandleFunc("/ws",func(w http.ResponseWriter, r *http.Request) {
		dcs.ServeWS(hub, w, r)
	})
	log.Println("listening on port",*addr)
	log.Fatal(http.ListenAndServe(*addr,nil))
}