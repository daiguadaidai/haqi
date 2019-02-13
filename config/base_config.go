package config

type BaseConfig struct {
	StartLogFile      string
	StartLogPos       uint32
	EndLogFile        string
	EndLogPos         uint32
	TransSchemas      []string
	TransTables       []string
	ThreadID          uint32
	EnableTransUpdate bool
	EnableTransInsert bool
	EnableTransDelete bool
	SchemaSuffix      string
}

// 是否有开始位点信息
func (this *BaseConfig) HaveStartPosInfo() bool {
	if this.StartLogFile == "" {
		return false
	}
	return true
}

// 是否所有结束位点信息
func (this *BaseConfig) HaveEndPosInfo() bool {
	if this.EndLogFile == "" {
		return false
	}
	return true
}
