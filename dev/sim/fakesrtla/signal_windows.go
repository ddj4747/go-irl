package main

import "time"

// Windows has no SIGUSR1; chaos mode remains available to simulate outages.
func setupOutageSignal(_ *sender, _ time.Duration) {}
