package manal

import (
	"fmt"
	"github.com/daiguadaidai/haqi/config"
	"github.com/daiguadaidai/haqi/dao"
	"github.com/daiguadaidai/haqi/models"
	"strings"
)

// 获取开始的位点信息
func GetStartPosition(bc *config.BaseConfig, dbc *config.DBConfig) (*models.Position, error) {
	if bc.HaveStartPosInfo() { // 有设置开始位点信息
		startPos := getPositionByPosInfo(bc.StartLogFile, bc.StartLogPos)
		// 检测开始位点是否在系统保留的binlog范围内
		if err := checkStartPosInRange(startPos, dbc); err != nil {
			return nil, err
		}
		return startPos, nil
	}

	return nil, nil
}

// 通过位点信息
func getPositionByPosInfo(logFile string, logPos uint32) *models.Position {
	return &models.Position{
		File:     logFile,
		Position: logPos,
	}
}

// 检测开始位点是否在系统保留的binlog范围内
func checkStartPosInRange(startPos *models.Position, dbc *config.DBConfig) error {
	// 获取 最老和最新的位点信息
	defaultDao, err := dao.NewDefaultDao(dbc.Host, dbc.Port)
	if err != nil {
		return err
	}
	oldestPos, newestPos, err := defaultDao.GetOldestAndNewestPos()
	if err != nil {
		return err
	}

	if startPos.LessThan(oldestPos) {
		return fmt.Errorf("指定的开始位点 %s:%d 太过久远. 存在最老的binlog为: %s:4",
			startPos.File, startPos.Position, oldestPos.File)
	}

	if newestPos.LessThan(startPos) {
		return fmt.Errorf("指定的开始位点 %s:%d 还没有生成. 存在最新的binlog为: %s:%d",
			startPos.File, startPos.Position, newestPos.File, newestPos.Position)
	}

	return nil
}

// 获取结束位点信息
func GetEndPosition(bc *config.BaseConfig) *models.Position {
	if bc.HaveEndPosInfo() {
		return getPositionByPosInfo(bc.EndLogFile, bc.EndLogPos)
	}
	return nil
}

/* 获取需要回滚的表
Return:
[
	{
        cchema: 数据库名,
        table: 表名
    },
    ......
]
*/
func FindTransTables(bc *config.BaseConfig, dbc *config.DBConfig) ([]*models.DBTable, TransType, error) {
	transTables := make([]*models.DBTable, 0, 1)

	// 没有指定表, 说明使用所有的表
	if len(bc.TransSchemas) == 0 && len(bc.TransTables) == 0 {
		return transTables, TransTypeAll, nil
	}

	notAllTableSchema := make(map[string]bool) // 如果指定的表中有指定cchema. 则代表该cchema不不要所有的表
	for _, table := range bc.TransTables {
		items := strings.Split(table, ".")
		switch len(items) {
		case 1: // table. 没有指定cchema, 只指定了table
			if len(bc.TransSchemas) == 0 { // 该表没有指定库
				return nil, TransTypeNone, fmt.Errorf("表:%v. 没有指定库", table)
			}
			for _, cchema := range bc.TransSchemas {
				if _, ok := notAllTableSchema[cchema]; !ok {
					notAllTableSchema[cchema] = true
				}

				t := models.NewDBTable(cchema, table)
				transTables = append(transTables, t)
			}
		case 2: // cchema.table 的格式, 代表有指定cchema 和 table
			if _, ok := notAllTableSchema[items[0]]; !ok {
				notAllTableSchema[items[0]] = true
			}
			t := models.NewDBTable(items[0], items[1])
			transTables = append(transTables, t)
		default:
			return nil, TransTypeNone, fmt.Errorf("不能识别需要执行的表: %v", table)
		}
	}

	// 要是指定的schema, 不存在于 notAllTableSchema 这个变量中, 说明这个cchema中的表都需要回滚
	for _, cchema := range bc.TransSchemas {
		if _, ok := notAllTableSchema[cchema]; ok {
			continue
		}
		notAllTableSchema[cchema] = true

		defaultDao, err := dao.NewDefaultDao(dbc.Host, dbc.Port)
		if err != nil {
			return nil, TransTypeNone, err
		}
		tables, err := defaultDao.FindTablesBySchema(cchema)
		if err != nil {
			return nil, TransTypeNone, fmt.Errorf("获取数据库下面的所有表失败. %v", err)
		}
		transTables = append(transTables, tables...)
	}

	if len(transTables) == 0 {
		return transTables, TransTypeAll, nil
	}

	return transTables, TransTypePartial, nil
}
