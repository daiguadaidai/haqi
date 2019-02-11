package gdbc

import (
	"sync"

	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/haqi/config"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
)

var instanceMap InstanceMap

type Instance struct {
	DB *gorm.DB
}

type InstanceMap struct {
	DBs sync.Map
	sync.Once
}

/* 单例模式获取原生数据库链接
Params:
    _host: ip
        string-> 127.0.0.1
    _port: 端口
        int-> 3306
Return:
    *DynamicInstance: 动态数据实例
    error: 错误信息
*/
func GetInstanceByHostPort(host string, port int) (*Instance, error) {
	key := fmt.Sprintf("%v:%v", host, port)
	instance, err := GetInstance(key)

	return instance, err
}

/* 单例模式获取原生数据库链接
Params:
    _dynamicKey: 获取配置文件的key
        string-> 127.0.0.1:3306
Return:
    *DynamicInstance: 动态数据实例
    error: 错误信息
*/
func GetInstance(key string) (*Instance, error) {
	var instance *Instance
	instanceInterface, ok := instanceMap.DBs.Load(key) // 获取动态实例

	if !ok { // 获取不到动态实例, 需要创建一个
		// 获取数据库实例配置信息
		cfg, ok := config.GetDBConfig(key)
		if !ok {
			errMsg := fmt.Sprintf("获取动态实例失败, 没有找到相关的实例配置信息, %v", key)
			return nil, fmt.Errorf(errMsg)
		}

		// 实例化元数据库实例
		instanceMap.Once.Do(func() {
			// 链接数据库
			var err error
			instance = new(Instance)

			seelog.Debugf("数据库链接描述符: %v", cfg.GetDataSource())

			instance.DB, err = gorm.Open("mysql", cfg.GetDataSource())
			if err != nil { // 打开数据库失败
				seelog.Errorf("打开动态数据库实例错误, key:%v, %v", key, err)
				return
			}

			instance.DB.DB().SetMaxOpenConns(cfg.MaxOpenConns)
			instance.DB.DB().SetMaxIdleConns(cfg.MaxIdelConns)

			// 将该实例链接保存在字典中
			instanceMap.DBs.Store(key, instance)
		})

		// 创建动态实例失败
		if instance == nil {
			return nil, fmt.Errorf("获取动态实例失败, 不能创建动态实例")
		}
	} else { // 将动态实例接口类型转化成动态实例类型
		instance = instanceInterface.(interface{}).(*Instance)
	}

	return instance, nil
}
