package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	srt "github.com/datarhei/gosrt"
)

// testSRTLAPublisher translates one local SRT connection to a registered
// SRTLA link, allowing the test to exercise the real public input socket.
type testSRTLAPublisher struct {
	local *net.UDPConn
	link  *net.UDPConn
	mu    sync.Mutex
	peer  *net.UDPAddr
}

func newTestSRTLAPublisher(t *testing.T, port int) *testSRTLAPublisher {
	t.Helper()
	remote := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}
	link, err := net.DialUDP("udp4", nil, remote)
	if err != nil {
		t.Fatal(err)
	}
	local, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		link.Close()
		t.Fatal(err)
	}
	s := &testSRTLAPublisher{local: local, link: link}
	t.Cleanup(func() { local.Close(); link.Close() })
	reg := make([]byte, SRTLAReg1Len)
	binary.BigEndian.PutUint16(reg, SRTLATypeReg1)
	if _, err := link.Write(reg); err != nil {
		t.Fatal(err)
	}
	_ = link.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, MTU)
	n, err := link.Read(buf)
	if err != nil || n != SRTLAReg2Len || binary.BigEndian.Uint16(buf) != SRTLATypeReg2 {
		t.Fatalf("SRTLA group registration: %d %v", n, err)
	}
	if _, err := link.Write(buf[:n]); err != nil {
		t.Fatal(err)
	}
	n, err = link.Read(buf)
	if err != nil || n != SRTLAReg3Len || binary.BigEndian.Uint16(buf) != SRTLATypeReg3 {
		t.Fatalf("SRTLA link registration: %d %v", n, err)
	}
	_ = link.SetReadDeadline(time.Time{})
	go func() {
		buf := make([]byte, MTU)
		for {
			n, addr, err := local.ReadFromUDP(buf)
			if err != nil {
				return
			}
			s.mu.Lock()
			s.peer = addr
			s.mu.Unlock()
			_, _ = link.Write(buf[:n])
		}
	}()
	go func() {
		buf := make([]byte, MTU)
		for {
			n, err := link.Read(buf)
			if err != nil {
				return
			}
			if n < SRTMinLen || binary.BigEndian.Uint16(buf) == SRTLATypeACK {
				continue
			}
			s.mu.Lock()
			peer := s.peer
			s.mu.Unlock()
			if peer != nil {
				_, _ = local.WriteToUDP(buf[:n], peer)
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if _, err := link.Write([]byte{0x90, 0}); err != nil {
				return
			}
		}
	}()
	return s
}

func startTestServer(t *testing.T, executable string, srtlaPort, srtPort int, id, passphrase string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(executable, "-cli", "-mode=server", fmt.Sprintf("-srtla-port=%d", srtlaPort), fmt.Sprintf("-srt-port=%d", srtPort), "-streamId="+id, "-passphrase="+passphrase)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	ready := make(chan bool, 1)
	var output strings.Builder
	var outputMu sync.Mutex
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			outputMu.Lock()
			output.WriteString(scanner.Text() + "\n")
			outputMu.Unlock()
			if strings.Contains(scanner.Text(), "Listening on shared SRTLA") {
				select {
				case ready <- true:
				default:
				}
			}
		}
		close(ready)
	}()
	select {
	case ok := <-ready:
		if !ok {
			outputMu.Lock()
			defer outputMu.Unlock()
			t.Fatalf("server exited before registering:\n%s", output.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatal("server did not register")
	}
	return cmd
}

func dialTestSRT(t *testing.T, addr, id, passphrase string) srt.Conn {
	t.Helper()
	config := srt.DefaultConfig()
	config.StreamId = id
	config.Passphrase = passphrase
	conn, err := srt.Dial("srt", addr, config)
	if err != nil {
		t.Fatalf("dial %q: %v", id, err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func readTestLinks(t *testing.T, conn srt.Conn) []linkStats {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(8 * time.Second))
	for {
		payload, err := readTelemetryFrame(conn)
		if err != nil {
			t.Fatalf("read telemetry: %v", err)
		}
		var message statsMessage
		if json.Unmarshal(payload, &message) == nil && message.Type == "reader" && len(message.Links) > 0 {
			return message.Links
		}
	}
}

func TestSharedRegistrationConflicts(t *testing.T) {
	b := &sharedBroker{routes: make(map[string]*sharedRegistration), registrations: make(map[string]*sharedRegistration), srtlaPort: 5000, srtPort: 5001}
	request := sharedRequest{SRTLAPort: 5000, SRTPort: 5001, StreamID: "alpha"}
	reg, err := b.register(request, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"alpha", "alpha-client", "alpha-stats"} {
		request.StreamID = id
		if _, err := b.register(request, nil); err == nil {
			t.Fatalf("registered conflicting stream ID %q", id)
		}
	}
	request.StreamID = "beta"
	request.SRTLAPort = 5002
	if _, err := b.register(request, nil); err == nil {
		t.Fatal("registered with a different port pair")
	}
	b.unregister(reg)
	request.SRTLAPort = 5000
	request.StreamID = "alpha"
	if _, err := b.register(request, nil); err != nil {
		t.Fatalf("could not reuse unregistered ID: %v", err)
	}
}

func TestSharedPortsAcrossServerProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("starts server processes")
	}
	executable := filepath.Join(t.TempDir(), "go-irl")
	if runtime.GOOS == "windows" {
		executable += ".exe"
	}
	goexe := filepath.Join(runtime.GOROOT(), "bin", "go")
	if runtime.GOOS == "windows" {
		goexe += ".exe"
	}
	build := exec.Command(goexe, "build", "-o", executable, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build server: %v\n%s", err, output)
	}
	srtlaPort, err := getFreePort()
	if err != nil {
		t.Fatal(err)
	}
	var srtPort int
	for srtPort == 0 {
		candidate, err := getFreePort()
		if err != nil {
			t.Fatal(err)
		}
		if candidate == srtlaPort {
			continue
		}
		listener, err := net.Listen("tcp4", sharedControlAddress(candidate))
		if err == nil {
			_ = listener.Close()
			srtPort = candidate
		}
	}
	// Allow the detached broker to release its executable before TempDir cleanup.
	t.Cleanup(func() {
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			conn, err := net.DialTimeout("tcp4", sharedControlAddress(srtPort), 100*time.Millisecond)
			if err != nil {
				return
			}
			conn.Close()
			time.Sleep(200 * time.Millisecond)
		}
	})
	first := startTestServer(t, executable, srtlaPort, srtPort, "alpha", "alpha-passphrase")
	startTestServer(t, executable, srtlaPort, srtPort, "beta", "beta-passphrase")
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(srtPort))
	alphaClient := dialTestSRT(t, addr, downstreamMediaStreamID("alpha"), "alpha-passphrase")
	betaClient := dialTestSRT(t, addr, downstreamMediaStreamID("beta"), "beta-passphrase")
	alphaStats := dialTestSRT(t, addr, downstreamStatsStreamIDFor("alpha"), "alpha-passphrase")
	betaStats := dialTestSRT(t, addr, downstreamStatsStreamIDFor("beta"), "beta-passphrase")
	wrongKey := srt.DefaultConfig()
	wrongKey.StreamId = downstreamMediaStreamID("alpha")
	wrongKey.Passphrase = "beta-passphrase"
	wrongKey.ConnectionTimeout = time.Second
	if conn, err := srt.Dial("srt", addr, wrongKey); err == nil {
		conn.Close()
		t.Fatal("alpha accepted beta's passphrase")
	}
	alphaSender := newTestSRTLAPublisher(t, srtlaPort)
	betaSender := newTestSRTLAPublisher(t, srtlaPort)
	alphaPub := dialTestSRT(t, alphaSender.local.LocalAddr().String(), "alpha", "alpha-passphrase")
	betaPub := dialTestSRT(t, betaSender.local.LocalAddr().String(), "beta", "beta-passphrase")
	for _, stream := range []struct {
		publisher srt.Conn
		client    srt.Conn
		value     byte
	}{{alphaPub, alphaClient, 0x41}, {betaPub, betaClient, 0x42}} {
		payload := bytes.Repeat([]byte{stream.value}, 7*188)
		if _, err := stream.publisher.Write(payload); err != nil {
			t.Fatal(err)
		}
		_ = stream.client.SetReadDeadline(time.Now().Add(5 * time.Second))
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(stream.client, got); err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("stream %c received incorrect data: %v", stream.value, err)
		}
	}
	alphaLinks := readTestLinks(t, alphaStats)
	betaLinks := readTestLinks(t, betaStats)
	if len(alphaLinks) != 1 || len(betaLinks) != 1 || alphaLinks[0].ID == betaLinks[0].ID {
		t.Fatalf("telemetry links crossed streams: alpha=%+v beta=%+v", alphaLinks, betaLinks)
	}
	if err := first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = first.Wait()
	payload := bytes.Repeat([]byte{0x43}, 7*188)
	if _, err := betaPub.Write(payload); err != nil {
		t.Fatal(err)
	}
	_ = betaClient.SetReadDeadline(time.Now().Add(5 * time.Second))
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(betaClient, got); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("second stream stopped when first process exited: %v", err)
	}
	control, err := net.DialTimeout("tcp4", sharedControlAddress(srtPort), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var greeting sharedResponse
	if err := json.NewDecoder(control).Decode(&greeting); err != nil {
		t.Fatal(err)
	}
	control.Close()
	broker, err := os.FindProcess(greeting.PID)
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Kill(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	var recovered srt.Conn
	for time.Now().Before(deadline) {
		config := srt.DefaultConfig()
		config.StreamId = downstreamMediaStreamID("beta")
		config.Passphrase = "beta-passphrase"
		config.ConnectionTimeout = time.Second
		recovered, err = srt.Dial("srt", addr, config)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if recovered == nil {
		t.Fatalf("server did not recover after broker exit: %v", err)
	}
	t.Cleanup(func() { recovered.Close() })
	recoveredSender := newTestSRTLAPublisher(t, srtlaPort)
	recoveredPub := dialTestSRT(t, recoveredSender.local.LocalAddr().String(), "beta", "beta-passphrase")
	if _, err := recoveredPub.Write(payload); err != nil {
		t.Fatal(err)
	}
	_ = recovered.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(recovered, got); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("stream did not resume after broker exit: %v", err)
	}
}
