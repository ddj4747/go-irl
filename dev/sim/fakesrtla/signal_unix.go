//go:build !windows

package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// SIGUSR1 takes every link down temporarily on Unix systems.
func setupOutageSignal(s *sender, outage time.Duration) {
	usr1 := make(chan os.Signal, 1)
	signal.Notify(usr1, syscall.SIGUSR1)
	go func() {
		for range usr1 {
			log.Printf("SIGUSR1: ALL LINKS DOWN for %s", outage)
			s.allDown.Store(true)
			time.Sleep(outage)
			s.allDown.Store(false)
			log.Print("SIGUSR1: all links restored")
		}
	}()
}
