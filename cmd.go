package main

import (
	"fmt"
	"github.com/abiosoft/ishell"
	"strconv"
	"time"
)

// 设置
var SetCmd = &ishell.Cmd{
	Name: "set",
	Func: func(c *ishell.Context) {

	},
}

// 主动分析磁盘结构
var ParseDirCmd = &ishell.Cmd{
	Name: "dir",
	Func: func(c *ishell.Context) {
		GCP.ParseRawDisk(*GCP.Conf)
	},
}

// 分析doc
var ExtractDocsCmd = &ishell.Cmd{
	Name: "doc",
	Func: func(c *ishell.Context) {
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
		go GCP.ExtractDocs(docNum)

		c.ProgressBar().Start()
		for {
			ready, total := GCP.LoadReadyDocCount()
			if docNum > 0 && docNum < total {
				total = docNum
			}
			if total == 0 || ready >= total {
				break
			}
			i := int(float32(ready) / float32(total) * 100.0)
			c.ProgressBar().Suffix(fmt.Sprint(" ", i, "%"))
			c.ProgressBar().Progress(i)
			time.Sleep(time.Second * 1)
		}
		c.ProgressBar().Stop()
	},
}

// 获取HTTP对象
var FindObjectCmd = &ishell.Cmd{
	Name: "find",
	Func: func(c *ishell.Context) {

	},
}

// 查看DIO状态
var StatDIOCmd = &ishell.Cmd{
	Name: "dio_stat",
	Func: func(c *ishell.Context) {
		fmt.Println(GCP.Dio.DumpStat())
	},
}

func AtsCmd() {
	shell := ishell.New()
	shell.Println("ATS DiskParser Shell")
	shell.AddCmd(ParseDirCmd)
	shell.AddCmd(StatDIOCmd)
	shell.AddCmd(ExtractDocsCmd)
	shell.Run()
}
