package config

import (
	"fmt"
	"github.com/cihub/seelog"
)

const (
	ENABLE_TRANS_UPDATE   = false
	ENABLE_TRANS_INSERT   = false
	ENABLE_TRANS_DELETE   = true
	DEFAULT_SCHEMA_SUFFIX = "_archive"
)

var sc *ToMySQLConfig

type ToMySQLConfig struct {
	BaseConfig
	APIConfig
}

func SetToMySQLConfig(cfg *ToMySQLConfig) {
	sc = cfg
}

func (this *ToMySQLConfig) Check() error {
	if err := this.checkCondition(); err != nil {
		return err
	}

	return nil
}

func (this *ToMySQLConfig) checkCondition() error {
	// 同时指定了开始位点和结束位点
	if this.HaveStartPosInfo() && this.HaveEndPosInfo() {
		// 判断开始位点是否大于结束位点
		if this.StartLogFile < this.EndLogFile {
			return nil
		} else if this.StartLogFile == this.EndLogFile { // 文件相等
			if this.StartLogPos < this.EndLogPos {
				return nil
			} else {
				return fmt.Errorf("指定的开始位点 %s:%d 大于结束位点 %s:%d (文件相等)",
					this.StartLogFile, this.StartLogPos, this.EndLogFile, this.EndLogPos)
			}
		} else { // 开始位点大于结束位点
			return fmt.Errorf("指定的开始位点 %s:%d 大于结束位点 %s:%d (文件大于)",
				this.StartLogFile, this.StartLogPos, this.EndLogFile, this.EndLogPos)
		}
	}

	if !this.HaveStartPosInfo() { // 没有指定开始位点
		return fmt.Errorf("没有指定开始位点")
	}

	// 到这里说明, 有开始位点,没有结束位点
	if !this.EnableReadAPI() {
		return fmt.Errorf("没有指定结束位点, 并且也没有指定使用读取数据的API/没有指定task uuid." +
			" 读取的API是用来获取结束位点的.")
	}
	seelog.Infof("TaskUUID: %s. 读取API为: %s", this.TaskUUID, this.ReadAPI)

	// 开启update api
	if this.EnableUpdateAPI() {
		seelog.Infof("TaskUUID: %s. 更新API为: %s", this.TaskUUID, this.UpdateAPI)
	}

	return nil
}
