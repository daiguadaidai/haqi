package config

type APIConfig struct {
	TaskUUID  string
	UpdateAPI string
	ReadAPI   string
}

// 是否启动 实时读取API信息
func (this *APIConfig) EnableReadAPI() bool {
	if len(this.TaskUUID) == 0 || len(this.ReadAPI) == 0 {
		return false
	}
	return true
}

// 是否启动 实时更新API信息
func (this *APIConfig) EnableUpdateAPI() bool {
	if len(this.TaskUUID) == 0 || len(this.UpdateAPI) == 0 {
		return false
	}
	return true
}
