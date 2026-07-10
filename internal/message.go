package dcs

type Envelope struct {
	Type       string `json:"type"`
	RoomID     string `json:"room_id"`
	FromUserID string `json:"from_user_id"`
	ToUserId   string `json:"to_user_id"` // 1:1 DMs	
	Body       string `json:"body"`
}

//returns redis channel to which env should be published 
func (e Envelope)Target()(channel string){
	switch{
	case e.RoomID!="":
		return "room:"+e.RoomID
	case e.ToUserId!="":
		return "user:"+e.ToUserId
	default:
		return ""
	}
}
