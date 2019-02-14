package types

import (
	"encoding/json"
	"fmt"
	"github.com/daiguadaidai/haqi/utils"
)

type TaskInfo struct {
	TaskUUID   string `json:"task_uuid" form:"task_uuid"`
	NotifyInfo string `json:"notify_info" form:"notify_info"`
	RealInfo   string `json:"real_info" form:"real_info"`
}

type ReadInfo struct {
	EndLogFile string `json:"end_log_file" form:"end_log_file"`
	EndLogPos  uint32 `json:"end_log_pos" form:"end_log_pos"`
}

type SaveInfo struct {
	ParseLogFile string `json:"parse_log_file" form:"parse_log_file"`
	ParseLogPos  uint32 `json:"parse_log_pos" form:"parse_log_pos"`
	ApplyLogFile string `json:"apply_log_file" form:"apply_log_file"`
	ApplyLogPos  uint32 `json:"apply_log_pos" form:"apply_log_pos"`
	EndLogFile   string `json:"end_log_file" form:"end_log_file"`
	EndLogPos    uint32 `json:"end_log_pos" form:"end_log_pos"`
}

func GetReadInfo(taskUUID string, api string) (*ReadInfo, error) {
	apiData, err := utils.GetURL(api, fmt.Sprintf("?task_uuid=%s", taskUUID))
	if err != nil {
		return nil, err
	}
	if data, ok := apiData.(map[string]interface{}); ok {
		if readInfoStr, haveData := data["notify_info"]; haveData {
			readInfo := new(ReadInfo)
			if err = json.Unmarshal([]byte(readInfoStr.(string)), readInfo); err != nil {
				return nil, fmt.Errorf("数据转化出错误. data: %v. %v", data, err)
			}
			return readInfo, nil
		}
	} else {
		return nil, fmt.Errorf("获取到未知数据: %T -> %v", data, data)
	}

	return nil, fmt.Errorf("没有获取到数据")
}

// 更新保存信息
func UpdateSaveInfo(taskUUID string, api string, saveInfo *SaveInfo) error {
	saveInfoStr, err := json.Marshal(saveInfo)
	if err != nil {
		return err
	}
	taskInfo := new(TaskInfo)
	taskInfo.TaskUUID = taskUUID
	taskInfo.RealInfo = string(saveInfoStr)

	if _, err = utils.PutURL(api, taskInfo); err != nil {
		return err
	}

	return nil
}
