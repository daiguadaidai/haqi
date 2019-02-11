package config

import (
	"fmt"
	"github.com/daiguadaidai/haqi/utils"
	"github.com/siddontang/go-mysql/replication"
	"strings"
	"sync"
)

const (
	DB_HOST           = "127.0.0.1"
	DB_PORT           = 3306
	DB_USERNAME       = "root"
	DB_PASSWORD       = "root"
	DB_SCHEMA         = ""
	DB_AUTO_COMMIT    = true
	DB_MAX_OPEN_CONNS = 100
	DB_MAX_IDEL_CONNS = 10
	DB_CHARSET        = "utf8mb4"
	DB_TIMEOUT        = 10
)

type DBConfig struct {
	Username          string
	Password          string
	Database          string
	CharSet           string
	Host              string
	Timeout           int
	Port              int
	MaxOpenConns      int
	MaxIdelConns      int
	AllowOldPasswords int
	AutoCommit        bool
}

func (this *DBConfig) GetDataSource() string {
	dataSource := fmt.Sprintf(
		"%v:%v@tcp(%v:%v)/%v?charset=%v&allowOldPasswords=%v&timeout=%vs&autocommit=%v&parseTime=True&loc=Local",
		this.Username,
		this.Password,
		this.Host,
		this.Port,
		this.Database,
		this.CharSet,
		this.AllowOldPasswords,
		this.Timeout,
		this.AutoCommit,
	)

	return dataSource
}

func (this *DBConfig) Check() error {
	if strings.TrimSpace(this.Database) == "" {
		return fmt.Errorf("数据库不能为空")
	}

	return nil
}

func (this *DBConfig) GetSyncerConfig() replication.BinlogSyncerConfig {
	return replication.BinlogSyncerConfig{
		ServerID: utils.RandRangeUint32(100000000, 200000000),
		Flavor:   "mysql",
		Host:     this.Host,
		Port:     uint16(this.Port),
		User:     this.Username,
		Password: this.Password,
	}
}

func (this *DBConfig) Addr() string {
	return fmt.Sprintf("%s:%d", this.Host, this.Port)
}

var configMap sync.Map

/* 保存数据库配置信息,
将配置信息的 host:port 为 key 将配置信息保存到 sync.Map 中
*/
func AddDBConfig(dbc *DBConfig) error {
	if dbc == nil {
		return fmt.Errorf("配置文件不能为 nil")
	}
	configMap.Store(dbc.Addr(), dbc)
	return nil
}

/* 通过host, port 获取数据库配置信息
Params:
    _host: ip
        string-> 127.0.0.1
    _port: 端口
        int-> 3306
*/
func GetDBConifgByHostPort(host string, port int) (*DBConfig, bool) {
	key := fmt.Sprintf("%v:%v", host, port)
	return GetDBConfig(key)
}

/* 获取数据库配置文件
通过给定的 key 从 sync.Map 中获取一个数据库配置信息
Params:
    _dynamicKey: 获取配置文件的key
        string-> 127.0.0.1:3306
Return:
    *setting.DBConfig: 数据库配置信息
    bool: 是否获取成功
*/
func GetDBConfig(key string) (*DBConfig, bool) {
	dbConfigInterface, ok := configMap.Load(key)
	if !ok {
		return nil, ok
	}

	return dbConfigInterface.(interface{}).(*DBConfig), ok
}
