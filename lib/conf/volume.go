/*
参考： https://docs.trafficserver.apache.org/en/5.3.x/reference/configuration/volume.config.en.html?highlight=volume.config#std:configfile-volume.config
*/

package conf

import (
	"fmt"
	"github.com/golang/glog"
	"io"
	"path/filepath"
	"strconv"
	"strings"
)

// 从volume.config解析而来
type ConfigVolumes struct {
	NumVolumes       int          `json:"num_volumes"`        // volume数目
	NumHttpVolumes   int          `json:"num_http_volumes"`   // http类型的volume
	NumStreamVolumes int          `json:"num_stream_volumes"` // 流媒体类型的volume
	CPQueue          []*ConfigVol `json:"-"`                  // 每个volume独特的配置
}

// 每个Volume特有的配置
type ConfigVol struct {
	Number    int   `json:"number"`
	Scheme    int   `json:"scheme"`
	Size      int64 `json:"size"`
	InPercent bool  `json:"in_percent"`
	Percent   int   `json:"percent"`
}

func (ac *ATSConfig) loadConfigVolumes() (*ConfigVolumes, error) {
	filename := filepath.Join(ac.Path, "volume.config")
	configReader, err := NewConfigReader(filename)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return nil, err
	}
	defer configReader.Close()

	cvs := &ConfigVolumes{
		NumVolumes: 0,
		CPQueue:    make([]*ConfigVol, 0),
	}
	for {
		a, c := configReader.ReadLine()
		if c == io.EOF {
			break
		}
		if len(a) == 0 {
			continue
		}
		// volume=1 scheme=http size=45%
		objs := strings.Fields(a)
		if len(objs) != 3 {
			continue
		}
		cv := ConfigVol{}
		// vol number
		volTag, volNumberStr, err := KVParse(objs[0])
		if volTag != "volume" || err != nil {
			glog.Errorf("unknown filed: %s", volTag)
			continue
		}
		volNumber, err := strconv.Atoi(volNumberStr)
		if err != nil || !(volNumber > 0 && volNumber < 255) {
			if err != nil {
				glog.Errorf("parse vol number failed: %s", err.Error())
			} else {
				glog.Error("parse vol number failed, vol number must between 0 - 255, but current %D", volNumber)
			}
			continue
		}
		cv.Number = volNumber

		// scheme
		schemeField, schemeValue, err := KVParse(objs[1])
		if schemeField != "scheme" || err != nil {
			glog.Errorf("unknown filed: %s", volTag)
			continue
		}
		if schemeValue == "http" {
			cv.Scheme = 1 // CACHE_HTTP_TYPE
		} else if schemeValue == "mixt" {
			cv.Scheme = 2 // CACHE_RTSP_TYPE
		} else {
			glog.Error("parse scheme failed")
			continue
		}

		// size
		sizeField, sizeValue, err := KVParse(objs[2])
		if sizeField != "size" || err != nil {
			glog.Errorf("unknown filed: %s", sizeField)
			continue
		}
		if (sizeValue[len(sizeValue)-1:]) == "%" {
			cv.InPercent = true
			size, err := strconv.Atoi(sizeValue[:len(sizeValue)-1])
			if err != nil {
				glog.Errorf("parse size 3 failed")
				continue
			}
			if size > 100 {
				glog.Errorf("parse size 4 failed")
				continue
			}
			cv.Percent = size
		} else {
			size, err := strconv.Atoi(sizeValue)
			if err != nil {
				glog.Errorf("parse size 2 failed")
				continue
			}
			cv.Size = int64(size)
			cv.InPercent = false
		}
		cvs.CPQueue = append(cvs.CPQueue, &cv)
		cvs.NumVolumes++
	}
	return cvs, nil
}
