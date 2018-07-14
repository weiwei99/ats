package main

import (
	"encoding/json"
	"fmt"
	"github.com/abiosoft/ishell"
	"github.com/weiwei99/ats/lib/cache"
	"net/url"
	"strconv"
	"time"
)

//var GCP *diskparser.CacheParser
var GATSClient *ATSClient

// 设置
var SetCmd = &ishell.Cmd{
	Name: "conf",
	Help: "conf /usr/local/etc/trafficserver",
	Func: func(c *ishell.Context) {
		if len(c.Args) != 1 {
			fmt.Println("must need one args")
			return
		}
		atsCli := NewATSClient()
		err := atsCli.LoadConfiguration(c.Args[0])
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Println(atsCli.AtsConf.Dump())
		GATSClient = atsCli
	},
}

// 以cache process为开始，内部会进行解析
var CacheProcessEntryCmd = &ishell.Cmd{
	Name: "process",
	Help: "类似 cache processor",
	Func: func(c *ishell.Context) {
		if GATSClient.CacheParser.Conf == nil {
			fmt.Println("use conf command first")
			return
		}
		proc, err := cache.NewCacheProcesser(GATSClient.AtsConf)
		if err != nil {
			fmt.Println(err.Error())
		}
		err = proc.Start()
		if err != nil {
			fmt.Println(err.Error())
		}
	},
}

// 主动分析磁盘结构
var ParseDirCmd = &ishell.Cmd{
	Name: "base",
	Func: func(c *ishell.Context) {
		if GATSClient.CacheParser.Conf == nil {
			fmt.Println("use conf command first")
			return
		}
		err := GATSClient.CacheParser.MainParse()
		if err != nil {
			fmt.Println(err.Error())
		}

	},
}

var DumpDiskCmd = &ishell.Cmd{
	Name: "disk",
	Func: func(c *ishell.Context) {
		if !GATSClient.IsInitialize() {
			fmt.Println("use conf command first")
			return
		}
		diskCount := len(GATSClient.CacheParser.Processor.CacheDisks)
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

		hd, err := json.Marshal(GATSClient.CacheParser.Processor.CacheDisks[n].Header)
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
		if !GATSClient.IsInitialize() {
			fmt.Println("use conf command first")
			return
		}
	},
}

// 分析doc
var ExtractDocsCmd = &ishell.Cmd{
	Name: "doc",
	Func: func(c *ishell.Context) {
		if !GATSClient.IsInitialize() {
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

		for _, v := range GATSClient.CacheParser.Processor.CacheDisks {
			go v.ExtractDocs(docNum)
		}

		c.ProgressBar().Start()

		start := time.Now()
		var ready, total int
		for {
			for _, v := range GATSClient.CacheParser.Processor.CacheDisks {
				dirReady, dirTotal := v.LoadReadyDirCount()
				ready += dirReady
				total += dirTotal
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
		if !GATSClient.IsInitialize() {
			fmt.Println("use set command first")
			return
		}
		if len(c.Args) != 1 {
			fmt.Println("need 1 arg")
			return
		}

		// 检查入参
		_, err := url.Parse(c.Args[0])
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		reurlString := GATSClient.RemapService.Remap(c.Args[0])
		doc, err := GATSClient.CacheParser.Processor.CacheDisks[0].FindURL(reurlString)
		if err != nil {
			fmt.Printf("find failed: %s\n", err)
		}
		docStr, _ := json.Marshal(doc)
		fmt.Println(string(docStr))
	},
}

// 查看DIO状态
var StatDIOCmd = &ishell.Cmd{
	Name: "dio_stat",
	Func: func(c *ishell.Context) {
		if !GATSClient.IsInitialize() {
			fmt.Println("use set_conf_dir command first")
			return
		}
		for _, v := range GATSClient.CacheParser.Processor.CacheDisks {
			fmt.Println(v.Dio.DumpStat())
			fmt.Println("-----------------------")
		}
	},
}

//
func welcome() string {
	return "ATS DiskParser Shell"
}

//
func CommandLoop() {
	shell := ishell.New()
	shell.Println(welcome())
	shell.AddCmd(SetCmd)
	shell.AddCmd(ParseDirCmd)
	shell.AddCmd(StatDIOCmd)
	shell.AddCmd(ExtractDocsCmd)
	shell.AddCmd(FindObjectCmd)
	shell.AddCmd(DumpDiskCmd)
	shell.AddCmd(DumpVolCmd)
	shell.AddCmd(CacheProcessEntryCmd)
	shell.Run()
}
