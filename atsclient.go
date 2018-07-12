package main

import (
	"fmt"
	"github.com/weiwei99/ats/diskparser"
	"github.com/weiwei99/ats/lib/conf"
	"github.com/weiwei99/ats/lib/proxy"
)

type ATSClient struct {
	CacheParser  *diskparser.CacheParser // cache分析器
	RemapService *proxy.RemapService     // remap服务
	AtsConf      *conf.ATSConfig         // 配置文件

	initialize bool
}

func NewATSClient() *ATSClient {

	cli := &ATSClient{
		initialize: false,
	}

	return cli
}

func (ac *ATSClient) IsInitialize() bool {

	return ac.initialize
}

func (ac *ATSClient) LoadConfiguration(path string) error {
	ac.initialize = false

	cnf, err := conf.NewAtsConfig(path)
	if err != nil {
		return fmt.Errorf("load config failed %s", err.Error())
	}
	ac.AtsConf = cnf

	// 创建分析器
	cp, err := diskparser.NewCacheParser(cnf)
	if err != nil {
		return fmt.Errorf("create cache parser fail: %s\n", err.Error())
	}
	cp.Conf = cnf
	// remap 服务
	rs := proxy.NewRemapService()
	err = rs.LoadRule(cnf.RemapConfigPath)
	if err != nil {
		return fmt.Errorf("%s\n", "load remap rule failed")
	}
	ac.RemapService = rs
	ac.CacheParser = cp
	ac.initialize = true
	return nil
}

func (ac *ATSClient) Run() {

}
