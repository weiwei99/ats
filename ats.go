package main

import (
	"flag"
	"fmt"
	"github.com/abiosoft/ishell"
	"github.com/golang/glog"
	"github.com/weiwei99/ats/diskparser"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
)

var GCP *diskparser.CacheParser
var GCONF *diskparser.Config

var ParseDirCmd = &ishell.Cmd{
	Name: "dir",
	Func: func(c *ishell.Context) {
		GCP.ParseRawDisk(*GCONF)
	},
}

var StatDirCmd = &ishell.Cmd{
	Name: "dir_stat",
	Func: func(c *ishell.Context) {
		GCP.DirStat()
	},
}

func cmd() {
	shell := ishell.New()
	shell.Println("ATS DiskParser Shell")
	shell.AddCmd(ParseDirCmd)
	shell.AddCmd(StatDirCmd)
	shell.Run()
}

func main() {

	conf := diskparser.Config{}
	flag.StringVar(&conf.Path, "path", "/dev/sdb", "-path=/dev/sdb")
	flag.IntVar(&conf.MinAverageObjectSize, "mos", 8000, "-min_average_object_size")
	flag.Parse()

	// 分析

	cp := diskparser.CacheParser{}
	dio := &diskparser.DiskReader{}
	cp.Dio = dio

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

		fmt.Println(cp.Dio.DumpStat())
		os.Exit(1)
		// ... do something ...
	}()

	//

	GCP = &cp
	GCONF = &conf
	//err := cp.ParseRawDisk(conf)
	//if err != nil {
	//	fmt.Println(err)
	//	return
	//}
	cmd()
}
