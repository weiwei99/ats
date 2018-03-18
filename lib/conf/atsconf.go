package conf

import (
	"fmt"
	"strings"
)

type ATSConfig struct {
	Path                 string
	MinAverageObjectSize int
	Storages             []string
	RemapConfigPath      string
	ConfigVolumes        *ConfigVolumes
}

func NewAtsConfig(path string) (*ATSConfig, error) {

	ac := &ATSConfig{
		Storages: []string{},
		Path:     path,
	}
	ac.Path = strings.TrimSuffix(ac.Path, "/")
	ac.Path += "/"

	// 加载cachevol配置
	configVols, err := ac.loadConfigVolumes()
	if err != nil {
		return nil, err
	}
	ac.ConfigVolumes = configVols

	//
	err = ac.loadRecords()
	if err != nil {
		return nil, err
	}
	//
	err = ac.loadStorage()
	if err != nil {
		return nil, err
	}
	//
	ac.RemapConfigPath = ac.Path + "remap.config"
	return ac, nil
}

func (ac *ATSConfig) isComment(a []byte) bool {
	b := strings.TrimSpace(string(a))
	if len(b) < 1 || string(b[0]) == "#" {
		return true
	}
	return false
}

func (ac *ATSConfig) Dump() string {
	var ret string
	ret = fmt.Sprintf("%s", "----Current ATS config----\n")
	ret += "Disk Info: \n"
	for i, s := range ac.Storages {
		ret += fmt.Sprintf("\tID:%d:\t%s\n", i, s)
	}
	ret += fmt.Sprintf("MinAverageObjectSize: %d\n", ac.MinAverageObjectSize)
	return ret
}
