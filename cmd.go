package main

import (
	"encoding/json"
	"fmt"
	"github.com/abiosoft/ishell"
	"github.com/weiwei99/ats/diskparser"
	"strconv"
	"time"
)

var GCP *diskparser.CacheParser

// 设置
var SetCmd = &ishell.Cmd{
	Name: "conf",
	Help: "conf /usr/local/etc/trafficserver",
	Func: func(c *ishell.Context) {
		if len(c.Args) != 1 {
			fmt.Println("must need one args")
			return
		}
		ac, err := diskparser.NewAtsConfig(c.Args[0])
		if err != nil {
			fmt.Printf("failed: %s\n", err.Error())
			return
		}
		fmt.Println(ac.Dump())

		// 创建分析器
		cp, err := diskparser.NewCacheParser(ac)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		GCP = cp
		GCP.Conf = ac
	},
}

// 主动分析磁盘结构
var ParseDirCmd = &ishell.Cmd{
	Name: "base",
	Func: func(c *ishell.Context) {
		if GCP.Conf == nil {
			fmt.Println("use conf command first")
			return
		}
		err := GCP.MainParse()
		if err != nil {
			fmt.Println(err.Error())
		}

	},
}

var DumpDiskCmd = &ishell.Cmd{
	Name: "disk",
	Func: func(c *ishell.Context) {
		if GCP.Conf == nil {
			fmt.Println("use conf command first")
			return
		}
		diskCount := len(GCP.CacheDisks)
		if diskCount == 0 {
			fmt.Println("没有找到缓存盘，检查ats配置文件")
			return
		}
		if len(c.Args) != 1 {
			fmt.Printf("需要一个参数，从0-%d\n", diskCount-1)
			return
		}

		n, err := strconv.Atoi(c.Args[0])
		if err != nil || n > (diskCount-1) {
			fmt.Printf("需要一个参数，从0-%d\n", diskCount-1)
			return
		}

		hd, err := json.Marshal(GCP.CacheDisks[n].Header)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Println(string(hd))
	},
}

var DumpVolCmd = &ishell.Cmd{
	Name: "vol",
	Func: func(c *ishell.Context) {
		if GCP.Conf == nil {
			fmt.Println("use conf command first")
			return
		}
	},
}

// 分析doc
var ExtractDocsCmd = &ishell.Cmd{
	Name: "doc",
	Func: func(c *ishell.Context) {
		if GCP.Conf == nil {
			fmt.Println("use conf command first")
			return
		}

		docNum := 0
		if len(c.Args) > 0 {
			n, err := strconv.Atoi(c.Args[0])
			if err != nil {
				fmt.Println("need args 0 is number")
				return
			}
			docNum = n
		}
		fmt.Println(c.Args)

		for _, v := range GCP.CacheDisks {
			go v.ExtractDocs(docNum)
		}

		c.ProgressBar().Start()

		start := time.Now()
		var ready, total int
		for {
			for _, v := range GCP.CacheDisks {
				diskReady, diskTotal := v.LoadReadyDocCount()
				ready += diskReady
				total += diskTotal
			}

			if docNum > 0 && docNum < total {
				total = docNum
			}
			if total == 0 || ready >= total {
				break
			}
			i := int(float32(ready) / float32(total) * 100.0)
			c.ProgressBar().Suffix(fmt.Sprint(
				" ", i, "%", " ", ready, "/", total, " ",
				int(time.Since(start).Seconds()), " seconds"))
			c.ProgressBar().Progress(i)
			time.Sleep(time.Second * 1)
			ready = 0
			total = 0
		}
		c.ProgressBar().Suffix(fmt.Sprint(
			" ", 100, "%", " ", ready, "/", total, " ",
			int(time.Since(start).Seconds()), " seconds"))
		c.ProgressBar().Progress(100)
		c.ProgressBar().Stop()
	},
}

// 获取HTTP对象
var FindObjectCmd = &ishell.Cmd{
	Name: "find",
	Func: func(c *ishell.Context) {
		if GCP.Conf == nil {
			fmt.Println("use set_conf_dir command first")
			return
		}
	},
}

// 查看DIO状态
var StatDIOCmd = &ishell.Cmd{
	Name: "dio_stat",
	Func: func(c *ishell.Context) {
		if GCP.Conf == nil {
			fmt.Println("use set_conf_dir command first")
			return
		}
		for _, v := range GCP.CacheDisks {
			fmt.Println(v.Dio.DumpStat())
			fmt.Println("-----------------------")
		}
	},
}

func welcome() string {
	return "ATS DiskParser Shell"
}

func AtsCmd() {
	shell := ishell.New()
	shell.Println(welcome())
	shell.AddCmd(SetCmd)
	shell.AddCmd(ParseDirCmd)
	shell.AddCmd(StatDIOCmd)
	shell.AddCmd(ExtractDocsCmd)
	shell.AddCmd(DumpDiskCmd)
	shell.AddCmd(DumpVolCmd)
	shell.Run()
}
