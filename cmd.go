package main

import (
	"fmt"
	"github.com/abiosoft/ishell"
	"github.com/weiwei99/ats/diskparser"
	"strconv"
	"time"
)

var GCP *diskparser.CacheParser

// 设置
var SetCmd = &ishell.Cmd{
	Name: "set_conf_dir",
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
			fmt.Println("use set_conf_dir command first")
			return
		}
		err := GCP.MainParse()
		if err != nil {
			fmt.Println(err.Error())
		}
	},
}

// 分析doc
var ExtractDocsCmd = &ishell.Cmd{
	Name: "doc",
	Func: func(c *ishell.Context) {
		if GCP.Conf == nil {
			fmt.Println("use set_conf_dir command first")
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
		var ready, total int

		for {
			for _, v := range GCP.CacheDisks {
				_ready, _total := v.LoadReadyDocCount()
				ready += _ready
				total += _total
			}

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

func AtsCmd() {
	shell := ishell.New()
	shell.Println("ATS DiskParser Shell")
	shell.AddCmd(SetCmd)
	shell.AddCmd(ParseDirCmd)
	shell.AddCmd(StatDIOCmd)
	shell.AddCmd(ExtractDocsCmd)
	shell.Run()
}
