// Measures:
//   1. Max sustained concurrent connections (watch failedConns as you scale -conns)
//   2. Message delivery latency p50/p95/p99 within a room

//claude generated

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type Envelope struct {
	Type       string `json:"type"`
	RoomID     string `json:"room_id"`
	FromUserID string `json:"from_user_id"`
	ToUserId   string `json:"to_user_id"`
	Body       string `json:"body"`
}

var (
	urlsFlag = flag.String(
	"urls",
	"ws://localhost:5000/ws",
	"comma-separated websocket endpoints",
	)


	numConns    = flag.Int("conns", 1000, "number of concurrent connections to open")
	numRooms    = flag.Int("rooms", 20, "number of rooms to distribute connections across")
	testDur     = flag.Duration("duration", 30*time.Second, "how long to run the message-send phase")
	sendEvery   = flag.Duration("send-interval", 500*time.Millisecond, "how often each connection sends a message")
	connectOnly = flag.Bool("connect-only", false, "only measure max sustained connections, skip message phase")
	rampDelay   = flag.Duration("ramp-delay", 2*time.Millisecond, "delay between successive connection attempts")

	successfulConns int64
	failedConns     int64
	messagesSent    int64
	messagesRecv    int64

	latenciesMu sync.Mutex
	latencies   []time.Duration

	gatewayURLs []string
)

func initURLs() {
	for _, u := range strings.Split(*urlsFlag, ",") {
		u = strings.TrimSpace(u)
		if u != "" {
			gatewayURLs = append(gatewayURLs, u)
		}
	}

	if len(gatewayURLs) == 0 {
		log.Fatal("no gateway urls supplied")
	}
}

func main() {
	flag.Parse()
	initURLs()
	log.Printf(
	"Starting load test: %d connections across %d rooms using %d gateways",
	*numConns,
	*numRooms,
	len(gatewayURLs),
)

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	for i := 0; i < *numConns; i++ {
	wg.Add(1)

	room := fmt.Sprintf("loadtest-room-%d", i%*numRooms)
	userID := fmt.Sprintf("client-%d", i)

	// Round-robin across gateways
	gateway := gatewayURLs[i%len(gatewayURLs)]

	go func(gateway, uid, roomID string) {
		defer wg.Done()
		runClient(gateway, uid, roomID, stopCh)
	}(gateway, userID, room)

	time.Sleep(*rampDelay)
}

	log.Printf("All connection attempts dispatched. Running for %s...", *testDur)
	time.Sleep(*testDur)
	close(stopCh)
	wg.Wait()

	report()
}

func buildDialURL(baseURL, userID string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}

	q := u.Query()
	q.Set("user", userID)
	u.RawQuery = q.Encode()

	return u.String(), nil
}
func runClient(baseURL, userID, room string, stopCh <-chan struct{}) {
	dialURL, err := buildDialURL(baseURL, userID)
	if err != nil {
		log.Fatalf("bad url: %v", err)
	}

	conn, _, err := websocket.DefaultDialer.Dial(dialURL, nil)
	if err != nil {
		atomic.AddInt64(&failedConns, 1)
		return
	}
	defer conn.Close()
	atomic.AddInt64(&successfulConns, 1)

	joinEnv := Envelope{Type: "join", RoomID: room}
	if err := conn.WriteJSON(joinEnv); err != nil {
		return
	}

	if *connectOnly {
		<-stopCh
		return
	}

	done := make(chan struct{})
	go readLoop(conn, userID, done)

	ticker := time.NewTicker(*sendEvery)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-done:
			return
		case <-ticker.C:
			body := strconv.FormatInt(time.Now().UnixNano(), 10) + "|ping"
			msg := Envelope{Type: "msg", RoomID: room, FromUserID: userID, Body: body}
			if err := conn.WriteJSON(msg); err != nil {
				return
			}
			atomic.AddInt64(&messagesSent, 1)
		}
	}
}

func readLoop(conn *websocket.Conn, selfID string, done chan<- struct{}) {
	defer close(done)
	for {
		var env Envelope
		if err := conn.ReadJSON(&env); err != nil {
			return
		}
		if env.Type != "msg" || env.FromUserID == selfID {
			continue
		}

		parts := strings.SplitN(env.Body, "|", 2)
		if len(parts) != 2 {
			continue
		}
		sentNano, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			continue
		}
		latency := time.Since(time.Unix(0, sentNano))
		atomic.AddInt64(&messagesRecv, 1)

		latenciesMu.Lock()
		latencies = append(latencies, latency)
		latenciesMu.Unlock()
	}
}

func report() {
	fmt.Println("\n=== Load Test Results ===")
	fmt.Printf("Successful connections: %d\n", successfulConns)
	fmt.Printf("Failed connections:     %d\n", failedConns)
	fmt.Printf("Messages sent:          %d\n", messagesSent)
	fmt.Printf("Messages received:      %d\n", messagesRecv)

	latenciesMu.Lock()
	defer latenciesMu.Unlock()

	if len(latencies) == 0 {
		fmt.Println("\nNo latency samples collected. Check:")
		fmt.Println("  - buildDialURL() matches how your handler reads userID")
		fmt.Println("  - join_req.roomID field name matches env.RoomID (json: room_id)")
		os.Exit(0)
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[minInt(len(latencies)*99/100, len(latencies)-1)]

	fmt.Printf("Latency p50: %v\n", p50)
	fmt.Printf("Latency p95: %v\n", p95)
	fmt.Printf("Latency p99: %v\n", p99)
	fmt.Println("\nFor your resume, e.g.:")
	fmt.Printf("\"Sustained %d+ concurrent connections with p99 message delivery latency of %v\"\n", successfulConns, p99)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var _ = json.Marshal
