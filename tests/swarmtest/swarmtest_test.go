package swarmtest

import (
	"testing"
)

func Test_RunSwarmTest(t *testing.T) {
	s := SwarmTest{}
	err := s.InitOptions(nil)
	if err != nil {
		panic(err)
	}
	s.Main()
}
