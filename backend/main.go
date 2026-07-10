package main

import (
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow cross-origin for desktop clients
	},
}

// Database connection removed for zero-dependency execution

// Hub maintains the active clients and broadcasts messages to clients in the same workspace.
type Hub struct {
	workspaces map[string]map[*Client]bool
	broadcast  chan Message
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
}

type Message struct {
	WorkspaceID string      `json:"workspaceId"`
	Type        string      `json:"type"` // "presence", "sync_op", "chat"
	SenderID    string      `json:"senderId"`
	SenderName  string      `json:"senderName"`
	Payload     interface{} `json:"payload"`
}

func newHub() *Hub {
	return &Hub{
		workspaces: make(map[string]map[*Client]bool),
		broadcast:  make(chan Message),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

// Logs transactions to stdout console
func logActivity(workspaceID, username, actionType, details string) {
	log.Printf("[Sync Log] Room: %s | User: %s | Action: %s | %s", workspaceID, username, actionType, details)
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			if _, ok := h.workspaces[client.workspaceID]; !ok {
				h.workspaces[client.workspaceID] = make(map[*Client]bool)
			}
			h.workspaces[client.workspaceID][client] = true
			log.Printf("Client %s registered to workspace %s", client.id, client.workspaceID)
			h.mu.Unlock()

			logActivity(client.workspaceID, client.username, "join_workspace", client.username+" connected to the workspace.")

			// Broadcast a request to all existing clients in this workspace to send their presence
			// so the newly joined client gets populated in their teammates list instantly.
			presenceReq := Message{
				WorkspaceID: client.workspaceID,
				Type:        "request_presence",
				SenderID:    "server",
				SenderName:  "Server",
			}
			h.mu.RLock()
			clients := h.workspaces[client.workspaceID]
			for c := range clients {
				if c.id != client.id {
					select {
					case c.send <- presenceReq:
					default:
						close(c.send)
						delete(clients, c)
					}
				}
			}
			h.mu.RUnlock()

		case client := <-h.unregister:
			h.mu.Lock()
			if clients, ok := h.workspaces[client.workspaceID]; ok {
				if _, ok := clients[client]; ok {
					delete(clients, client)
					close(client.send)
					log.Printf("Client %s unregistered from workspace %s", client.id, client.workspaceID)
					if len(clients) == 0 {
						delete(h.workspaces, client.workspaceID)
					}
				}
			}
			h.mu.Unlock()

			logActivity(client.workspaceID, client.username, "leave_workspace", client.username+" disconnected.")

		case message := <-h.broadcast:
			h.mu.RLock()
			clients := h.workspaces[message.WorkspaceID]
			for client := range clients {
				if client.id != message.SenderID { // Don't echo back to sender
					select {
					case client.send <- message:
					default:
						close(client.send)
						delete(clients, client)
					}
				}
			}
			h.mu.RUnlock()

			// Log Sync Deltas to database
			if message.Type == "sync_op" {
				if payloadMap, ok := message.Payload.(map[string]interface{}); ok {
					relPath := payloadMap["path"]
					opType := payloadMap["op_type"]
					details := usernameStr(message.SenderName) + " " + stringVal(opType) + "ed " + stringVal(relPath)
					logActivity(message.WorkspaceID, message.SenderName, "file_sync", details)
				}
			}
		}
	}
}

// Helpers for interface mapping safely
func stringVal(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
func usernameStr(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return "User"
}

type Client struct {
	hub         *Hub
	conn        *websocket.Conn
	send        chan Message
	workspaceID string
	id          string
	username    string
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	for {
		var msg Message
		err := c.conn.ReadJSON(&msg)
		if err != nil {
			log.Printf("Read error: %v", err)
			break
		}
		// Secure sender metrics
		msg.SenderID = c.id
		msg.SenderName = c.username
		msg.WorkspaceID = c.workspaceID

		c.hub.broadcast <- msg
	}
}

func (c *Client) writePump() {
	defer func() {
		c.conn.Close()
	}()

	for {
		msg, ok := <-c.send
		if !ok {
			c.conn.WriteMessage(websocket.CloseMessage, []byte{})
			return
		}

		err := c.conn.WriteJSON(msg)
		if err != nil {
			log.Printf("Write error: %v", err)
			return
		}
	}
}

func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Upgrade error: %v", err)
		return
	}

	workspaceID := r.URL.Query().Get("workspaceId")
	clientID := r.URL.Query().Get("clientId")
	username := r.URL.Query().Get("username")

	if workspaceID == "" || clientID == "" {
		http.Error(w, "Missing workspaceId or clientId query parameter", http.StatusBadRequest)
		conn.Close()
		return
	}
	if username == "" {
		username = "Anonymous Developer"
	}

	client := &Client{
		hub:         hub,
		conn:        conn,
		send:        make(chan Message, 256),
		workspaceID: workspaceID,
		id:          clientID,
		username:    username,
	}
	client.hub.register <- client

	go client.writePump()
	go client.readPump()
}

func main() {
	log.Println("Starting OneWorkspace synchronization server...")

	hub := newHub()
	go hub.run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"healthy","service":"sync-server"}`))
	})

	port := ":8080"
	log.Printf("WebSocket Server listening on %s", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("ListenAndServe error: %v", err)
	}
}
