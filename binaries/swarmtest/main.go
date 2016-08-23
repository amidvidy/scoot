package main

import (
	swarmtest "github.com/scootdev/scoot/tests/swarmtest"
)

// Runs an end to end integration test with work being scheduled
// via the ScootApi placed on the WorkQueue, Dequeued by the Scheduler
// and ran on local instances of Workers.
func main() {
	s := swarmtest.SwarmTest{}
	err := s.InitOptions(nil)
	if err != nil {
		panic(err)
	}
	s.Main()
}
