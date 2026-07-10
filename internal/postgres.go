package dcs

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Message struct{
	ID int64
	RoomID *string
	FromUserID string
	ToUserId *string 
	Body string
	CreatedAt time.Time
}

type Postgres struct{
	pool *pgxpool.Pool
}

func NewPg(ctx context.Context,url string)(*Postgres,error){
	pool,err:=pgxpool.New(ctx,url)
	if err!=nil{
		return nil,err
	}
	if err:=pool.Ping(ctx);err!=nil{
		return nil,err
	}

	return &Postgres{pool:pool},nil
}

func (p *Postgres)saveMessage(ctx context.Context,m Message)error{
	_,err:=p.pool.Exec(ctx,
		`INSERT INTO messages (room_id, from_user_id, to_user_id, body) VALUES ($1, $2, $3, $4)`, m.RoomID,m.FromUserID,m.ToUserId,m.Body,
	)
	return err
}

// func (p *Postgres)close(){
// 	p.pool.Close()
// }