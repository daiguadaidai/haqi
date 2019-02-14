package manal

import (
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/haqi/config"
	"github.com/daiguadaidai/haqi/dao"
	"github.com/daiguadaidai/haqi/models"
	"github.com/daiguadaidai/haqi/utils"
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
	return getPositionByPosInfo("", 0)
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

// 检测和修复表
func CompareAndRePairTable(
	oriDBC *config.DBConfig,
	stdDBC *config.DBConfig,
	sName string,
	sSuffix string, // 数据库后缀
	tName string,
) error {
	var oriTableStr string
	var stdTableStr string
	var exists bool // 表是否存在
	var err error

	// 1. 获取源和目标表结构, 源表不存在则返回错误. 目标表不存在则创建一个新的.
	oriDao, err := dao.NewDefaultDao(oriDBC.Host, oriDBC.Port)
	if err != nil {
		return fmt.Errorf("获取源实例dao. %v", err)
	}
	// 获取源实例中的键表结构
	oriTableStr, exists, err = oriDao.ShowCreateTable(sName, tName)
	if err != nil {
		return fmt.Errorf("源实例show create table. %v", err)
	}
	if !exists { // 表不存在
		return fmt.Errorf("表:%s.%s在源实例中不存在%s", sName, tName, oriDBC.Addr())
	}

	stdSName := fmt.Sprintf("%s%s", sName, sSuffix) // 目标数据库名称
	stdDao, err := dao.NewDefaultDao(stdDBC.Host, stdDBC.Port)
	if err != nil {
		return fmt.Errorf("获取目标实例dao. %v", err)
	}
	// 创建目标数据库, create database if not exists xxx
	err = stdDao.ReCreateDB(fmt.Sprintf("%s%s", sName, sSuffix))
	if err != nil {
		return fmt.Errorf("创建目标数据库出错. %v", err)
	}
	// 获取目标数据库中的表结构
	stdTableStr, exists, err = stdDao.ShowCreateTable(stdSName, tName)
	if err != nil {
		return fmt.Errorf("目标实例show create table. %v", err)
	}
	if !exists { // 目标实例数据库中不存在表则创建相关表
		stdTableStr = utils.ReplaceCreateTableName(oriTableStr, stdSName, tName)
		if err = stdDao.CreateTable(stdTableStr); err != nil {
			return fmt.Errorf("创建目标数据库表 %v. %v", stdTableStr, err)
		}
		return nil
	}

	// 2. 获取原表和目标表的字段 crc32 值, 并且进行比较. 找到不一样或者多的字段
	// 获取源表字段 crc32
	oriColumnCRC32Map, err := oriDao.ColumnCRC32(sName, tName)
	if err != nil {
		return fmt.Errorf("获取源表%s.%s字段CRC32值. %v", sName, tName, err)
	}
	stdColumnCRC32Map, err := oriDao.ColumnCRC32(stdSName, tName)
	if err != nil {
		return fmt.Errorf("获取目标表%s.%s字段CRC32值. %v", stdSName, tName, err)
	}

	needAddColumns, needModifyColumns, err := compareColumn(oriColumnCRC32Map, stdColumnCRC32Map)
	if err != nil {
		return fmt.Errorf("目标表:%s.%s, 源表:%s.%s. %v", sName, tName, stdSName, tName, err)
	}

	// 执行 alter table add column sql语句
	addColumnSQLs := filterAddColumnSqls(oriTableStr, stdSName, tName, needAddColumns, len(oriColumnCRC32Map))
	for _, addSQL := range addColumnSQLs {
		err = stdDao.AlterTable(addSQL)
		if err != nil {
			return fmt.Errorf("表:%s.%s 添加字段失败. %s. %v", stdSName, tName, addSQL, err)
		}
		seelog.Infof("表:%s.%s 添加字段成功. %s", stdSName, tName, addSQL)
	}

	// 执行 alter table modify column sql语句
	modifyColumnSQLs := filterModifyColumnSqls(oriTableStr, stdSName, tName, needModifyColumns, len(oriColumnCRC32Map))
	for _, modifySQL := range modifyColumnSQLs {
		err = stdDao.AlterTable(modifySQL)
		if err != nil {
			return fmt.Errorf("表:%s.%s 添加字段失败. %s. %v", stdSName, tName, modifySQL, err)
		}
		seelog.Infof("表:%s.%s modify字段成功. %s", stdSName, tName, modifySQL)
	}

	return nil
}

// 比较字段crc32
func compareColumn(oriColumnMap, stdColumnMap map[string]int64) (map[string]bool, map[string]bool, error) {
	// 比较源表和目标表字段个数
	if len(oriColumnMap) < len(stdColumnMap) { // 目标表字段 多余 源表字段
		return nil, nil, fmt.Errorf("获取目标表字段 多余 源表字段. 请确认是否需要删除目标表字段")
	}

	needAddColumns := make(map[string]bool)
	needModifyColumns := make(map[string]bool)
	// 循环源表字段和目标表字段进行比较 看是否有缺少的字段和属性不一样的
	for cName, oriCRC32 := range oriColumnMap {
		if stdCRC32, ok := stdColumnMap[cName]; ok {
			if oriCRC32 != stdCRC32 {
				needModifyColumns[cName] = true
			}
		} else {
			needAddColumns[cName] = true
		}
	}

	// 循环目标表比较在目标有, 源表没有的
	for cName, _ := range stdColumnMap {
		if _, ok := oriColumnMap[cName]; !ok {
			return nil, nil, fmt.Errorf("字段:%s目标表中有, 源表中没有, 请确认目标表是否需要删除该字段", cName)
		}
	}

	return needAddColumns, needModifyColumns, nil
}

// 过滤获取添加字段语句
func filterAddColumnSqls(createTableSQL, sName, tName string, needAddColumns map[string]bool, cnt int) []string {
	addColumnSQLs := make([]string, len(needAddColumns))
	if len(needAddColumns) == 0 {
		return addColumnSQLs
	}

	items := strings.Split(createTableSQL, "\n")
	// 循环建表语句每一行, 获取字段名判断是否是需要添加的字段
	for i := 1; i <= cnt; i++ {
		cName := strings.Split(items[i], "`")[1]
		if _, ok := needAddColumns[cName]; ok {
			addSQL := fmt.Sprintf("ALTERT TABLE `%s`.`%s` ADD COLUMN %s", sName, tName, items[i])
			addColumnSQLs = append(addColumnSQLs, addSQL)
		}
	}

	return addColumnSQLs
}

func filterModifyColumnSqls(createTableSQL, sName, tName string, needModifyColumns map[string]bool, cnt int) []string {
	modifyColumnSQLs := make([]string, len(needModifyColumns))
	if len(needModifyColumns) == 0 {
		return modifyColumnSQLs
	}

	items := strings.Split(createTableSQL, "\n")
	// 循环建表语句每一行, 获取字段名判断是否是需要添加的字段
	for i := 1; i <= cnt; i++ {
		cName := strings.Split(items[i], "`")[1]
		if _, ok := needModifyColumns[cName]; ok {
			addSQL := fmt.Sprintf("ALTERT TABLE `%s`.`%s` MODIFY %s", sName, tName, items[i])
			modifyColumnSQLs = append(modifyColumnSQLs, addSQL)
		}
	}

	return modifyColumnSQLs
}
