package main

import (
	"flag"
	"github.com/golang/glog"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
)

func main() {
	var path string
	//conf := diskparser.AConfig{}
	flag.StringVar(&path, "ats_conf_dir", "/usr/local/etc/trafficserver",
		"-ats_conf_dir=/usr/local/etc/trafficserver")
	flag.Parse()

	// 捕获到panic异常
	defer func() {
		if err := recover(); err != nil {
			stack := debug.Stack()
			glog.Errorf("Ops!!! panic happened: %s", err)
			glog.Errorf("stack details: \n%s", string(stack))
		}
		glog.Flush()
		os.Exit(2)
	}()

	// catch signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		os.Interrupt,
		syscall.SIGHUP,  // 1
		syscall.SIGINT,  // 2
		syscall.SIGQUIT, // 3
		syscall.SIGTERM, // 15
		syscall.SIGABRT, // 6
		syscall.SIGILL,  // 4
		syscall.SIGFPE,  // 8
		syscall.SIGSEGV, // 11
	)
	go func() {
		sig := <-sigChan
		glog.Errorf("catch signal: %s", sig.String())
		glog.Flush()

		os.Exit(1)
		// ... do something ...
	}()

	CommandLoop()
}
