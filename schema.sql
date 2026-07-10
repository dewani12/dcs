CREATE TABLE messages(
    id BIGSERIAL PRIMARY KEY,
    room_id TEXT,
    from_user_id TEXT NOT NULL,
    to_user_id TEXT,
    body TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_messages_room ON messages(room_id,id DESC) WHERE room_id IS NOT NULL;
CREATE INDEX idx_messages_dm ON messages (least(from_user_id,to_user_id), greatest(from_user_id,to_user_id), id DESC) WHERE to_user_id IS NOT NULL;
