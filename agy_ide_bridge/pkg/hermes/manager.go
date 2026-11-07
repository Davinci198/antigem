// Package hermes bridges the Hermes Agent CLI with the AntiGem app.
//
// REST surface (under /api/hermes/):
//
//	POST   /api/hermes/sessions                          create a session → {session_id}
//	POST   /api/hermes/sessions/{id}/send                send a turn (JSON body: {content})
//	POST   /api/hermes/sessions/{id}/abort               interrupt the running turn
//	POST   /api/hermes/sessions/{id}/delete              remove a session
//	GET    /api/hermes/sessions/{id}/history             past messages → {messages:[{role,content,id}]}
//	GET    /api/hermes/sessions/{id}/stream              WebSocket: NDJSON event stream
//
// The WS stream emits:
//
//	{"type":"status",         "message":"…"}
//	{"type":"assistant_start"}
//	{"type":"assistant_delta","delta":"…"}
//	{"type":"assistant_done", "usage":{…}, "title":"…"}
//	{"type":"error",           "message":"…"}
package hermes

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// Manager handles all Hermes sessions.
type Manager struct {
	mu        sync.Mutex
	sessions  map[string]*Session
	hermesBin string // path to the hermes CLI
	homeDir   string
	upgrader  websocket.Upgrader
}

// NewManager creates a Manager. hermesBin may be empty (we search PATH).
func NewManager(homeDir, hermesBin string) *Manager {
	if hermesBin == "" || hermesBin == "~" {
		hermesBin = filepath.Join(homeDir, ".local", "bin", "hermes")
	} else if strings.HasPrefix(hermesBin, "~/") {
		hermesBin = filepath.Join(homeDir, hermesBin[2:])
	}
	return &Manager{
		sessions:  make(map[string]*Session),
		hermesBin: hermesBin,
		homeDir:   homeDir,
		upgrader: websocket.Upgrader{
			CheckOrigin:     func(*http.Request) bool { return true },
			ReadBufferSize:  4096,
			WriteBufferSize: 65536,
		},
	}
}

// session returns or creates a session.
func (m *Manager) session(id string) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[id]; ok {
		return s
	}
	s := &Session{
		id:        id,
		msgs:      []Message{},
		subs:      map[*sub]struct{}{},
		abortCh:   make(chan struct{}, 1),
		hermesBin: m.hermesBin,
		homeDir:   m.homeDir,
	}
	m.sessions[id] = s
	return s
}

func (m *Manager) delete(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[id]; ok {
		s.abort()
	}
	delete(m.sessions, id)
}

func (m *Manager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.sessions {
		s.abort()
	}
}

var idRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// ServeHTTP routes /api/hermes/ requests.
func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// strip prefix
	path := strings.TrimPrefix(r.URL.Path, "/api/hermes")
	path = strings.TrimPrefix(path, "/")

	switch {
	case path == "sessions" && r.Method == http.MethodPost:
		m.handleCreate(w, r)

	default:
		// /sessions/{id}/…
		parts := strings.SplitN(path, "/", 3)
		if len(parts) < 3 || parts[0] != "sessions" || !idRe.MatchString(parts[1]) {
			writeJSON(w, http.StatusNotFound, obj{"error": "not found"})
			return
		}
		id, sub := parts[1], parts[2]
		sess := m.session(id)
		switch {
		case sub == "send" && r.Method == http.MethodPost:
			m.handleSend(w, r, sess)
		case sub == "abort" && r.Method == http.MethodPost:
			m.handleAbort(w, sess)
		case sub == "delete" && r.Method == http.MethodPost:
			m.delete(id)
			writeJSON(w, http.StatusOK, obj{"success": true})
		case sub == "history" && r.Method == http.MethodGet:
			m.handleHistory(w, sess)
		case sub == "stream":
			m.handleStream(w, r, sess)
		default:
			writeJSON(w, http.StatusNotFound, obj{"error": "unknown endpoint"})
		}
	}
}

// ── handlers ─────────────────────────────────────────────────────────────────

func (m *Manager) handleCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ConversationID string `json:"conversation_id"`
		Title          string `json:"title"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	id := uuid.New().String()
	sess := m.session(id)
	sess.mu.Lock()
	sess.conversationID = body.ConversationID
	sess.title = body.Title
	sess.mu.Unlock()
	writeJSON(w, http.StatusOK, obj{"session_id": id})
}

func (m *Manager) handleSend(w http.ResponseWriter, r *http.Request, sess *Session) {
	var body struct {
		Content string `json:"content"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Content == "" {
		writeJSON(w, http.StatusBadRequest, obj{"error": "missing content"})
		return
	}
	go sess.runTurn(body.Content)
	writeJSON(w, http.StatusOK, obj{"success": true})
}

func (s *Session) abort() {
	select {
	case s.abortCh <- struct{}{}:
	default:
	}
}

func (m *Manager) handleAbort(w http.ResponseWriter, sess *Session) {
	sess.abort()
	writeJSON(w, http.StatusOK, obj{"success": true})
}

func (m *Manager) handleHistory(w http.ResponseWriter, sess *Session) {
	sess.mu.Lock()
	msgs := make([]Message, len(sess.msgs))
	copy(msgs, sess.msgs)
	sess.mu.Unlock()
	writeJSON(w, http.StatusOK, obj{"messages": msgs})
}

func (m *Manager) handleStream(w http.ResponseWriter, r *http.Request, sess *Session) {
	conn, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	_ = conn.UnderlyingConn().SetDeadline(time.Time{})

	s := &sub{conn: conn, ch: make(chan []byte, 512), done: make(chan struct{})}
	sess.mu.Lock()
	sess.subs[s] = struct{}{}
	sess.mu.Unlock()

	go s.writeLoop()
	defer func() {
		sess.mu.Lock()
		delete(sess.subs, s)
		sess.mu.Unlock()
		s.close()
	}()
	// keep alive until client disconnects
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

// ── Session ───────────────────────────────────────────────────────────────────

// Message is a stored turn (user or assistant).
type Message struct {
	ID      string `json:"id"`
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Session is one conversation thread with Hermes.
type Session struct {
	mu             sync.Mutex
	id             string
	conversationID string
	title          string
	msgs           []Message
	subs           map[*sub]struct{}
	abortCh        chan struct{}
	hermesBin      string
	homeDir        string
	// resume flag: the Hermes session id reported by --pass-session-id
	hermesSessionID string
}

func (s *Session) broadcast(b []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for sub := range s.subs {
		sub.send(b)
	}
}

func (s *Session) emit(v interface{}) {
	b, _ := json.Marshal(v)
	s.broadcast(b)
}

func (s *Session) runTurn(content string) {
	// store user message
	userMsg := Message{ID: uuid.New().String(), Role: "user", Content: content}
	s.mu.Lock()
	s.msgs = append(s.msgs, userMsg)
	hermesID := s.hermesSessionID
	s.mu.Unlock()

	s.emit(obj{"type": "status", "message": "Starting Hermes…"})

	args := []string{
		"chat",
		"--format", "stream-json",
		"--oneshot",
		"-q", content,
		"--pass-session-id",
		"--accept-hooks",
		"--yolo",
	}
	if hermesID != "" {
		args = append(args, "--resume", hermesID)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Watch for abort signal
	go func() {
		select {
		case <-s.abortCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	cmd := exec.CommandContext(ctx, s.hermesBin, args...)
	cmd.Dir = s.homeDir
	stderr := &tailBuffer{max: 8 * 1024}
	cmd.Stderr = stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		s.emit(obj{"type": "error", "message": fmt.Sprintf("spawn failed: %v", err)})
		return
	}
	if err := cmd.Start(); err != nil {
		s.emit(obj{"type": "error", "message": fmt.Sprintf("hermes start: %v", err)})
		return
	}
	log.Printf("[hermes] session %s pid %d", s.id, cmd.Process.Pid)

	// parse NDJSON from hermes --format stream-json
	assistantText := strings.Builder{}
	msgID := uuid.New().String()
	startedAssistant := false
	doneSent := false
	var usage interface{}

	emitAssistantStart := func() {
		if startedAssistant {
			return
		}
		startedAssistant = true
		s.emit(obj{"type": "assistant_start"})
	}
	appendDelta := func(delta string) {
		if delta == "" {
			return
		}
		emitAssistantStart()
		assistantText.WriteString(delta)
		s.emit(obj{"type": "assistant_delta", "delta": delta})
	}
	emitDone := func() {
		if doneSent {
			return
		}
		doneSent = true
		s.emit(obj{"type": "assistant_done", "usage": usage, "title": s.derivedTitle(content)})
		if assistantText.Len() == 0 {
			return
		}
		s.mu.Lock()
		s.msgs = append(s.msgs, Message{ID: msgID, Role: "assistant", Content: assistantText.String()})
		s.mu.Unlock()
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4*1024*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		var ev map[string]interface{}
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}
		evType, _ := ev["type"].(string)
		switch evType {
		case "session_created", "system":
			// stream-json reports the session id in system/init; keep it so the
			// next turn resumes the same Hermes session instead of starting over.
			if sid, ok := ev["session_id"].(string); ok && sid != "" {
				s.mu.Lock()
				s.hermesSessionID = sid
				s.mu.Unlock()
			}
		case "assistant", "text":
			// legacy schema used assistant/content; stream-json uses text/text
			delta, _ := ev["content"].(string)
			if delta == "" {
				delta, _ = ev["text"].(string)
			}
			appendDelta(delta)

		case "tool_use", "tool":
			// show tool name as status
			name, _ := ev["name"].(string)
			if name != "" {
				s.emit(obj{"type": "status", "message": "🔧 " + name})
			}

		case "result":
			// The gateway assigns the session id after system/init, so the
			// result envelope is the authoritative place to learn it.
			if sid, ok := ev["session_id"].(string); ok && sid != "" {
				s.mu.Lock()
				s.hermesSessionID = sid
				s.mu.Unlock()
			}
			// result.text is the complete reply; use it when no deltas arrived
			// (replay gap, or providers that only report the final answer).
			if txt, ok := ev["text"].(string); ok && txt != "" && assistantText.Len() == 0 {
				appendDelta(txt)
			}
			if u := usageFromTokens(ev["tokens"]); u != nil {
				usage = u
			}
			if msg, _ := ev["error"].(string); msg != "" {
				s.emit(obj{"type": "error", "message": msg})
			} else if code := jsonInt(ev["exit_code"]); code != 0 {
				s.emit(obj{"type": "error", "message": exitMessage(code, stderr.String())})
			}
			emitDone()

		case "error":
			msg, _ := ev["message"].(string)
			if msg == "" {
				msg, _ = ev["error"].(string)
			}
			if msg == "" {
				msg = "hermes error"
			}
			s.emit(obj{"type": "error", "message": msg})
		}
	}

	_ = stdout.Close()
	waitErr := cmd.Wait()
	if !doneSent {
		// Surface provider/auth failures instead of a silent empty reply. An
		// aborted turn cancels ctx, so it is not reported as an error.
		if waitErr != nil && ctx.Err() == nil {
			s.emit(obj{"type": "error", "message": exitMessage(exitCodeOf(waitErr), stderr.String())})
		}
		emitDone()
	}
}

// tailBuffer keeps the last max bytes written to it so a failed Hermes run can
// report the end of stderr without buffering an unbounded log.
type tailBuffer struct {
	buf []byte
	max int
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.max {
		t.buf = t.buf[len(t.buf)-t.max:]
	}
	return len(p), nil
}

func (t *tailBuffer) String() string { return string(t.buf) }

func jsonInt(v interface{}) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	}
	return 0
}

// usageFromTokens maps stream-json result.tokens onto the token shape the app
// renders (input_tokens/output_tokens).
func usageFromTokens(v interface{}) interface{} {
	tokens, ok := v.(map[string]interface{})
	if !ok {
		return nil
	}
	return obj{
		"input_tokens":  jsonInt(tokens["input"]),
		"output_tokens": jsonInt(tokens["output"]),
		"total_tokens":  jsonInt(tokens["total"]),
	}
}

func exitCodeOf(err error) int {
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return 1
}

func exitMessage(code int, stderr string) string {
	stderr = strings.TrimSpace(stderr)
	if stderr == "" {
		return fmt.Sprintf("hermes exited with code %d", code)
	}
	lines := strings.Split(stderr, "\n")
	if len(lines) > 6 {
		lines = lines[len(lines)-6:]
	}
	return fmt.Sprintf("hermes exited with code %d: %s", code, strings.Join(lines, " | "))
}

// derivedTitle truncates the first user message into a conversation title.
func (s *Session) derivedTitle(firstMsg string) string {
	if s.title != "" && s.title != "New Chat" {
		return s.title
	}
	r := []rune(firstMsg)
	if len(r) > 48 {
		r = append(r[:45], []rune("…")...)
	}
	title := string(r)
	s.mu.Lock()
	s.title = title
	s.mu.Unlock()
	return title
}

// ── subscriber ────────────────────────────────────────────────────────────────

type sub struct {
	conn *websocket.Conn
	ch   chan []byte
	done chan struct{}
	once sync.Once
}

func (s *sub) send(b []byte) {
	select {
	case <-s.done:
	case s.ch <- b:
	default:
		s.close()
	}
}

func (s *sub) close() {
	s.once.Do(func() {
		close(s.done)
		_ = s.conn.Close()
	})
}

func (s *sub) writeLoop() {
	for {
		select {
		case <-s.done:
			return
		case b := <-s.ch:
			if err := s.conn.WriteMessage(websocket.TextMessage, b); err != nil {
				s.close()
				return
			}
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

type obj = map[string]interface{}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// ensure io import is used (scanner uses it internally through stdout pipe)
var _ io.ReadCloser
