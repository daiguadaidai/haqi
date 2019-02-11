package types

import (
	"encoding/json"
	"fmt"
	"github.com/daiguadaidai/haqi/utils"
)

type RealInfo interface {
	ToJSON() (string, error)
}

type CreateRealInfo struct {
	RollbackSQLFile string `json:"rollback_sql_file" form:"rollback_sql_file"`
	OriSQLFile      string `json:"ori_sql_file" form:"ori_sql_file"`
	Host            string `json:"host" form:"host"`
	Port            int    `json:"port" form:"port"`
}

func (this *CreateRealInfo) ToJSON() (string, error) {
	bytes, err := json.Marshal(this)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

type ExecuteRealInfo struct {
	RollbackSQLFile string `json:"rollback_sql_file" form:"rollback_sql_file"`
	Host            string `json:"host" form:"host"`
	Port            int    `json:"port" form:"port"`
}

func (this *ExecuteRealInfo) ToJSON() (string, error) {
	bytes, err := json.Marshal(this)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

type PutData struct {
	TaskUUID   string `json:"task_uuid" form:"task_uuid"`
	NotifyInfo string `json:"notify_info" form:"notify_info"`
	RealInfo   string `json:"real_info" form:"real_info"`
}

type result struct {
	Status  bool     `form:"status" json:"status"`
	Message string   `form:"message" json:"message"`
	Data    *PutData `form:"data" json:data`
}

func NewPutDataByURL(url string, taskUUID string) (*PutData, error) {
	query := fmt.Sprintf("?task_uuid=%s", taskUUID)
	raw, err := utils.GetURLRaw(url, query)
	if err != nil {
		return nil, err
	}

	rs := new(result)
	if err = json.Unmarshal(raw, rs); err != nil {
		return nil, err
	}

	if !rs.Status {
		return nil, fmt.Errorf("%v", rs.Message)
	}

	return rs.Data, nil
}

// 获取 realinfo
func (this *PutData) GetRealInfo(obj interface{}) (interface{}, error) {
	if err := json.Unmarshal([]byte(this.RealInfo), obj); err != nil {
		return nil, err
	}

	return obj, nil
}
