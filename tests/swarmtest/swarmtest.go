package swarmtest

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type SwarmTest struct {
	Running    []*exec.Cmd
	LogDir     string
	RepoDir    string
	NumWorkers int
	Compile    func() error
	Run        func() error
	NumJobs    int
	Timeout    time.Duration
	mutex      *sync.Mutex
}

func (s *SwarmTest) InitOptions(defaults map[string]interface{}) error {
	d := map[string]interface{}{
		"logdir":      "",
		"repo":        "$GOPATH/src/github.com/scootdev/scoot",
		"num_workers": 10,
		"num_jobs":    10,
		"timeout":     10 * time.Second,
	}
	for key, val := range defaults {
		d[key] = val
	}
	logDir := flag.String("logdir", d["logdir"].(string), "If empty, write to stdout, else to log file")
	repoDir := flag.String("repo", d["repo"].(string), "Path to scoot repo")
	numWorkers := flag.Int("num_workers", d["num_workers"].(int), "Number of workerserver instances to spin up.")
	numJobs := flag.Int("num_jobs", d["num_jobs"].(int), "Number of Jobs to run")
	timeout := flag.Duration("timeout", d["timeout"].(time.Duration), "Time to wait for jobs to complete")

	flag.Parse()
	if *numWorkers < 5 {
		return errors.New("Need >5 workers (see scheduler.go:getNumNodes)")
	}

	s.LogDir = *logDir
	s.RepoDir = *repoDir
	s.NumWorkers = *numWorkers
	s.Compile = func() error { return s.compile() }
	s.Run = func() error { return s.run() }
	s.NumJobs = *numJobs
	s.Timeout = *timeout
	s.mutex = &sync.Mutex{}
	return nil
}

func (s *SwarmTest) Fatalf(format string, v ...interface{}) {
	fmt.Printf(format+"\n", v...)
	s.Kill()
}

func (s *SwarmTest) Kill() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, cmd := range s.Running {
		if cmd.ProcessState == nil && cmd.Process != nil {
			fmt.Printf("Killing: %v\n", cmd.Path)
			p, _ := os.FindProcess(cmd.Process.Pid)
			p.Signal(os.Interrupt)
		}
	}
}

func (s *SwarmTest) GetPort() int {
	l, _ := net.Listen("tcp", "localhost:0")
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func (s *SwarmTest) RunCmd(blocking bool, name string, args ...string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for i, _ := range args {
		args[i] = os.ExpandEnv(args[i])
	}
	cmd := exec.Command(os.ExpandEnv(name), args...)
	cmd.Dir = os.ExpandEnv(s.RepoDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	s.Running = append(s.Running, cmd)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("Error starting %v,%v: %v", name, args, err)
	}
	fmt.Println("Started: ", name, args)

	if !blocking {
		return nil
	} else {
		return cmd.Wait()
	}
}

func (s *SwarmTest) SetupLog(logDir string) error {
	if logDir != "" {
		if err := os.MkdirAll(logDir, 0777); err != nil {
			return fmt.Errorf("Could not create log dir: %v", logDir)
		}
		logpath := path.Join(logDir, "swarmtest"+strconv.Itoa(os.Getpid()))
		out, err := os.Create(logpath)
		if err != nil {
			return fmt.Errorf("Could not init logfile: %v", logpath)
		}
		fmt.Println("Redirecting to log: ", logpath)
		os.Stdout = out
		os.Stderr = out
	}
	return nil
}

func (s *SwarmTest) StartSignalHandler() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		var sig os.Signal
		sig = <-sigchan
		fmt.Printf("signal %s received, shutting down\n", sig)
		s.Kill()
		os.Exit(1)
	}()
}

func (s *SwarmTest) compile() error {
	return s.RunCmd(true, "go", "install", "./binaries/...")
}

func (s *SwarmTest) run() error {
	for i := 0; i < s.NumWorkers; i++ {
		thriftPort := strconv.Itoa(s.GetPort())
		httpPort := strconv.Itoa(s.GetPort())
		args := []string{"-thrift_port", thriftPort, "-http_port", httpPort}
		if err := s.RunCmd(false, "$GOPATH/bin/workerserver", args...); err != nil {
			return err
		}
	}
	args := []string{"-sched_config", `{"Cluster": {"Type": "local"}}`}
	if err := s.RunCmd(false, "$GOPATH/bin/scheduler", args...); err != nil {
		return err
	}

	return s.RunCmd(true, "$GOPATH/bin/scootapi", "run_smoke_test", strconv.Itoa(s.NumJobs), s.Timeout.String())
}

func (s *SwarmTest) RunSwarmTest() error {
	var err error
	s.StartSignalHandler()
	if err = s.SetupLog(s.LogDir); err == nil {
		fmt.Println("Compiling...")
		if err = s.Compile(); err == nil {
			fmt.Println("Running...")
			err = s.Run()
			s.Kill()
		}
	}
	fmt.Println("Done")
	return err
}

func (s *SwarmTest) Main() {
	err := s.RunSwarmTest()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	} else {
		os.Exit(0)
	}
}
