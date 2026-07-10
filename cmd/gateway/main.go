package main

import (
	"context"
	"flag"
	"log"
	"net/http"

	dcs "github.com/dewani12/dcs/internal"
)

func main(){
	addr:=flag.String("addr",":5000","http service address")
	redisAddr:=flag.String("redis", "localhost:6379", "redis address")
	pgUrl:=flag.String("pg","postgres://admin:admin@localhost:5432/dcs","pg url")
	flag.Parse()

	pg,err:=dcs.NewPg(context.Background(),*pgUrl)
	if err!=nil{
		log.Fatal("pg connect:",err)
	}

	wr:=dcs.NewWriter(pg,1000)

	hub:=dcs.NewHub()
	b:= dcs.NewBroker(*redisAddr,hub)
	
	hub.SetBroker(b)
	hub.SetWriter(wr)

	go b.Run()
	go hub.Run()

	http.HandleFunc("/ws",func(w http.ResponseWriter, r *http.Request) {
		dcs.ServeWS(hub, w, r)
	})
	log.Println("listening on port",*addr)
	log.Fatal(http.ListenAndServe(*addr,nil))
}