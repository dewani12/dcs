package dcs

type Envelope struct {
	Type       string `json:"type"`
	RoomID     string `json:"room_id"`
	FromUserID string `json:"from_user_id"`
	Body       string `json:"body"`
}
