package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"

	srt "github.com/datarhei/gosrt"
)

// A broker owns the public sockets. Server processes keep a local control
// connection open for as long as their stream should be registered.
type sharedBroker struct {
	mu            sync.Mutex
	routes        map[string]*sharedRegistration
	registrations map[string]*sharedRegistration
	srtlaPort     int
	srtPort       int
	lastEmpty     time.Time
	closing       bool
}

type sharedRegistration struct {
	token string
	relay *srtRelay
	conns map[srt.Conn]struct{}
}

type sharedRequest struct {
	Operation  string
	SRTLAPort  int
	SRTPort    int
	StreamID   string
	Passphrase string
	Token      string
}

type sharedResponse struct {
	Protocol  string
	SRTLAPort int
	SRTPort   int
	PID       int
	Error     string
	Log       string
	Token     string
	Snapshot  relaySnapshot
	Groups    []srtlaGroupInfo
}

func sharedControlAddress(port int) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
}

func (b *sharedBroker) accept(req srt.ConnRequest) {
	b.mu.Lock()
	reg := b.routes[req.StreamId()]
	if reg == nil && isLoopbackAddr(req.RemoteAddr()) {
		reg = b.routes[""]
	}
	if reg == nil {
		b.mu.Unlock()
		req.Reject(srt.REJ_PEER)
		return
	}
	mode := reg.relay.handleConnect(req)
	if mode == srt.REJECT {
		b.mu.Unlock()
		req.Reject(srt.REJ_PEER)
		return
	}
	conn, err := req.Accept()
	if err != nil {
		b.mu.Unlock()
		return
	}
	reg.conns[conn] = struct{}{}
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(reg.conns, conn)
		b.mu.Unlock()
	}()
	if mode == srt.PUBLISH {
		reg.relay.handlePublish(conn)
	} else {
		reg.relay.handleSubscribe(conn)
	}
}

func (b *sharedBroker) register(req sharedRequest, logMessage func(string)) (*sharedRegistration, error) {
	if req.SRTLAPort != b.srtlaPort || req.SRTPort != b.srtPort {
		return nil, fmt.Errorf("shared server is already using a different port pair")
	}
	ids := []string{req.StreamID, downstreamMediaStreamID(req.StreamID), downstreamStatsStreamIDFor(req.StreamID)}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closing {
		return nil, fmt.Errorf("shared listener is shutting down")
	}
	for _, id := range ids {
		if b.routes[id] != nil {
			return nil, fmt.Errorf("stream ID %q conflicts with a registered stream", id)
		}
	}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return nil, err
	}
	reg := &sharedRegistration{token: hex.EncodeToString(secret[:]), relay: newSRTRelay(req.Passphrase, req.StreamID), conns: make(map[srt.Conn]struct{})}
	reg.relay.logMessage = logMessage
	for _, id := range ids {
		b.routes[id] = reg
	}
	b.registrations[reg.token] = reg
	return reg, nil
}

func (b *sharedBroker) unregister(reg *sharedRegistration) {
	b.mu.Lock()
	for id, current := range b.routes {
		if current == reg {
			delete(b.routes, id)
		}
	}
	delete(b.registrations, reg.token)
	if len(b.registrations) == 0 {
		b.lastEmpty = time.Now()
	}
	connections := make([]srt.Conn, 0, len(reg.conns))
	for conn := range reg.conns {
		connections = append(connections, conn)
	}
	b.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
}

func (b *sharedBroker) handleControl(conn net.Conn) {
	defer conn.Close()
	encoder := json.NewEncoder(conn)
	if encoder.Encode(sharedResponse{Protocol: "go-irl-shared-v1", SRTLAPort: b.srtlaPort, SRTPort: b.srtPort, PID: os.Getpid()}) != nil {
		return
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	decoder := json.NewDecoder(io.LimitReader(conn, 8192))
	var req sharedRequest
	if decoder.Decode(&req) != nil {
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	if req.Operation == "snapshot" {
		b.mu.Lock()
		reg := b.registrations[req.Token]
		b.mu.Unlock()
		if reg == nil {
			_ = encoder.Encode(sharedResponse{Error: "stream is no longer registered"})
			return
		}
		reg.relay.mu.Lock()
		publisher := reg.relay.publisher
		reg.relay.mu.Unlock()
		var groups []srtlaGroupInfo
		if publisher != nil {
			groups = srtlaSnapshotForPublisher(publisher.RemoteAddr())
		}
		_ = encoder.Encode(sharedResponse{Snapshot: reg.relay.snapshot(), Groups: groups})
		return
	}
	if req.Operation != "register" {
		return
	}
	logs := make(chan string, 128)
	reg, err := b.register(req, func(message string) {
		select {
		case logs <- message:
		default:
		}
	})
	if err != nil {
		_ = encoder.Encode(sharedResponse{Error: err.Error()})
		return
	}
	defer b.unregister(reg)
	if encoder.Encode(sharedResponse{Token: reg.token}) != nil {
		return
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case message := <-logs:
				_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if encoder.Encode(sharedResponse{Log: message}) != nil {
					_ = conn.Close()
					return
				}
			case <-done:
				return
			}
		}
	}()
	// A closed control connection means that the owning process has exited.
	_, _ = io.Copy(io.Discard, conn)
}

func runSharedBroker(srtlaPort, srtPort int) error {
	control, err := net.Listen("tcp4", sharedControlAddress(srtPort))
	if err != nil {
		return err
	}
	defer control.Close()
	b := &sharedBroker{routes: make(map[string]*sharedRegistration), registrations: make(map[string]*sharedRegistration), srtlaPort: srtlaPort, srtPort: srtPort, lastEmpty: time.Now()}
	config := newSRTConfig()
	listener, err := srt.Listen("srt", net.JoinHostPort("0.0.0.0", strconv.Itoa(srtPort)), config)
	if err != nil {
		return err
	}
	defer listener.Close()
	go func() {
		for {
			req, err := listener.Accept2()
			if err != nil {
				_ = control.Close()
				return
			}
			go b.accept(req)
		}
	}()
	if err := startSrtla(uint(srtlaPort), "127.0.0.1", uint(srtPort), false); err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			b.mu.Lock()
			expired := len(b.registrations) == 0 && time.Since(b.lastEmpty) > 10*time.Second
			if expired {
				b.closing = true
			}
			b.mu.Unlock()
			if expired {
				_ = control.Close()
				return
			}
		}
	}()
	for {
		conn, err := control.Accept()
		if err != nil {
			return nil
		}
		go b.handleControl(conn)
	}
}

type sharedClient struct {
	conn    net.Conn
	address string
	token   string
	decoder *json.Decoder
}

func readSharedHello(conn net.Conn, srtlaPort, srtPort int) error {
	var response sharedResponse
	decoder := json.NewDecoder(conn)
	if err := decoder.Decode(&response); err != nil {
		return fmt.Errorf("read shared listener greeting: %w", err)
	}
	if response.Protocol != "go-irl-shared-v1" || response.SRTLAPort != srtlaPort || response.SRTPort != srtPort {
		return fmt.Errorf("a different service or SRTLA port is using the shared control port")
	}
	return nil
}

func registerSharedServer(ctx context.Context, cfg config) (*sharedClient, error) {
	address := sharedControlAddress(cfg.SRTPort)
	var conn net.Conn
	var err error
	dialer := net.Dialer{Timeout: 200 * time.Millisecond}
	conn, err = dialer.DialContext(ctx, "tcp4", address)
	if err != nil {
		executable, exeErr := os.Executable()
		if exeErr != nil {
			return nil, exeErr
		}
		cmd := exec.Command(executable, "--internal-shared-broker", strconv.Itoa(cfg.SRTLAPort), strconv.Itoa(cfg.SRTPort))
		if err := cmd.Start(); err != nil {
			return nil, fmt.Errorf("start shared listener: %w", err)
		}
		go func() { _ = cmd.Wait() }()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			conn, err = dialer.DialContext(ctx, "tcp4", address)
			if err == nil {
				break
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	if err != nil {
		return nil, fmt.Errorf("connect to shared listener: %w", err)
	}
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := readSharedHello(conn, cfg.SRTLAPort, cfg.SRTPort); err != nil {
		conn.Close()
		return nil, err
	}
	request := sharedRequest{Operation: "register", SRTLAPort: cfg.SRTLAPort, SRTPort: cfg.SRTPort, StreamID: cfg.StreamID, Passphrase: cfg.Passphrase}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		conn.Close()
		return nil, err
	}
	var response sharedResponse
	decoder := json.NewDecoder(conn)
	if err := decoder.Decode(&response); err != nil {
		conn.Close()
		return nil, fmt.Errorf("register with shared listener: %w", err)
	}
	if response.Error != "" {
		conn.Close()
		return nil, fmt.Errorf("register with shared listener: %s", response.Error)
	}
	_ = conn.SetDeadline(time.Time{})
	return &sharedClient{conn: conn, address: address, token: response.Token, decoder: decoder}, nil
}

func (c *sharedClient) snapshot() (relaySnapshot, []srtlaGroupInfo) {
	conn, err := net.DialTimeout("tcp4", c.address, time.Second)
	if err != nil {
		return relaySnapshot{}, nil
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	// The port pair was verified when this registration was established.
	var greeting sharedResponse
	if json.NewDecoder(conn).Decode(&greeting) != nil || greeting.Protocol != "go-irl-shared-v1" {
		return relaySnapshot{}, nil
	}
	if json.NewEncoder(conn).Encode(sharedRequest{Operation: "snapshot", Token: c.token}) != nil {
		return relaySnapshot{}, nil
	}
	var response sharedResponse
	if json.NewDecoder(conn).Decode(&response) != nil {
		return relaySnapshot{}, nil
	}
	return response.Snapshot, response.Groups
}

func runSharedServer(ctx context.Context, cfg config, hooks runHooks) error {
	client, err := registerSharedServer(ctx, cfg)
	if err != nil {
		return err
	}
	log.Printf("[server mode] SRTLA input UDP :%d  Downstream SRT UDP :%d", cfg.SRTLAPort, cfg.SRTPort)
	for {
		if hooks.onRelay != nil {
			hooks.onRelay(&srtRelay{shared: client})
		}
		log.Printf("Listening on shared SRTLA UDP :%d", cfg.SRTLAPort)
		done := make(chan struct{})
		go func(decoder *json.Decoder) {
			var event sharedResponse
			for decoder.Decode(&event) == nil {
				if event.Log != "" {
					log.Print(event.Log)
				}
				event = sharedResponse{}
			}
			close(done)
		}(client.decoder)
		select {
		case <-ctx.Done():
			_ = client.conn.Close()
			return nil
		case <-done:
			_ = client.conn.Close()
			log.Print("Shared listener disconnected; reconnecting")
		}
		for {
			client, err = registerSharedServer(ctx, cfg)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("Shared listener reconnect failed: %v", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}
		}
	}
}
