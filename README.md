## DCS

DCS is an event-driven, horizontally-scalable real-time chat server written in Go. It supports 1:1 direct messages and group chat rooms, fans out messages across multiple server instances using Redis pub/sub, persists messages to PostgreSQL, and tracks user presence.

![ss](https://github-production-user-asset-6210df.s3.amazonaws.com/145996845/620524239-fcef44f9-56cb-4a29-a86d-1e7d94a5f376.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260712%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260712T075922Z&X-Amz-Expires=300&X-Amz-Signature=1b9d7c935058a261811969533611534857c40a75e56be86a5bc62515ffb8e658&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

## Quickstart (dev)

The repository includes a `docker-compose.yml` that brings up Redis and Postgres for local development. From the repository root run:

```bash
docker-compose up -d
```

Create the database schema:

```bash
psql "postgres://admin:admin@localhost:5432/dcs" -f schema.sql
```
Run server (try multiple with different addr):

```bash
go run ./cmd/gateway -addr :5000
```

## Configuration

The gateway accepts the following flags (see `cmd/gateway/main.go`):

- `-addr` : HTTP service address (default `:5000`)
- `-redis` : Redis address for pub/sub (default `localhost:6379`)
- `-pg` : Postgres connection URL (default `postgres://admin:admin@localhost:5432/dcs`)

---

## API

WebSocket endpoint:

```
ws://localhost:5000/ws?user=alice
```

Presence (HTTP) endpoint:

```
http://localhost:5000/presence?user=alice
```

Message format (JSON):

1) Direct message (DM)

```json
{
    "type": "msg",
    "from_user_id": "alice",
    "to_user_id": "bob",
    "room_id": "",
    "body": "welcome"
}
```

2) Group chat message

```json
{
    "type": "msg",
    "from_user_id": "alice",
    "to_user_id": "",
    "room_id": "photon",
    "body": "welcome to photon community"
}
```

Supported `type` values: `msg`, `join`, `leave`.

Message format for join and leave group chat room(JSON): 
```json
{
    "type": "join",
    "from_user_id": "alice",
    "room_id": "photon"
}
```
---

## Database

Schema is provided in `schema.sql`. It creates a `messages` table and indexes used for efficient room and DM queries. Apply it with `psql` as shown above.

## Roadmap
 
- [ ] JWT auth (replacing `?user=`)
- [ ] Missed message replay on reconnect
- [ ] Multi node docker compose deployment

[def]: https://github.com/user-attachments/assets/fcef44f9-56cb-4a29-a86d-1e7d94a5f376