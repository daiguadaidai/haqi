package manal

import (
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/haqi/config"
	"syscall"
)

func Start(tmc *config.ToMySQLConfig, odbc *config.DBConfig, tdbc *config.DBConfig) {
	defer seelog.Flush()
	logger, _ := seelog.LoggerFromConfigAsBytes([]byte(config.LogDefautConfig()))
	seelog.ReplaceLogger(logger)

	if err := tmc.Check(); err != nil {
		seelog.Error(err.Error())
		syscall.Exit(1)
	}

	config.SetToMySQLConfig(tmc)
	if err := config.AddDBConfig(odbc); err != nil { // 添加源数据库配置文件
		seelog.Error(err.Error())
		syscall.Exit(1)
	}
	if err := config.AddDBConfig(tdbc); err != nil { // 添加目标配数据库置文件
		seelog.Error(err.Error())
		syscall.Exit(1)
	}

	manal, err := NewManal(tmc, odbc, tdbc)
	if err != nil {
		seelog.Error(err.Error())
		syscall.Exit(1)
	}

	if err := manal.Start(); err != nil {
		seelog.Error(err)
		syscall.Exit(1)
	}
}
