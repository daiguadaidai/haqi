package schema

import (
	"fmt"
	"github.com/daiguadaidai/haqi/dao"
	"github.com/daiguadaidai/haqi/utils"
	"github.com/ngaut/log"
	"strings"
)

type PKType int

var (
	PKTypeAllColumns PKType = 10
	PKTypePK         PKType = 20
	PKTypeUK         PKType = 20
)

type Table struct {
	SchemaName                     string
	SchemaSuffix                   string // schema后缀
	TableName                      string
	ColumnNames                    []string
	ColumnPos                      map[string]int // 每个字段对应的slice位置
	PKColumnNames                  []string       // 主键的所有字段
	PKType                                        // 主键类型 全部列. 主键. 唯一键
	InsertTemplate                 string         // insert sql 模板
	InsertValuePlaceholderTemplate string         // insert value 块的占位符
	UpdateTemplate                 string         // update sql 模板
	DeleteTemplate                 string         // delete sql 模板
}

func (this *Table) String() string {
	return fmt.Sprintf("%s.%s", this.GetSchema(), this.TableName)
}

func NewTable(sName string, sSuffix string, tName string, host string, port int) (*Table, error) {
	t := new(Table)
	t.SchemaName = sName
	t.SchemaSuffix = sSuffix
	t.TableName = tName

	dao, err := dao.NewDefaultDao(host, port)
	if err != nil {
		return nil, err
	}
	// 添加字段
	if err := t.addColumnNames(dao); err != nil {
		return nil, err
	}

	// 添加主键
	if err := t.addPK(dao); err != nil {
		return nil, err
	}

	t.initColumnPos()

	t.initSQLTemplate()

	return t, nil
}

func (this *Table) GetSchema() string {
	return fmt.Sprintf("%s%s", this.SchemaName, this.SchemaSuffix)
}

// 添加表的所有字段名
func (this *Table) addColumnNames(dao *dao.DefaultDao) error {
	var err error
	if this.ColumnNames, err = dao.FindTableColumnNames(this.GetSchema(), this.TableName); err != nil {
		return err
	}

	if len(this.ColumnNames) == 0 {
		return fmt.Errorf("表:%s 没有获取到字段, 请确认指定表是否不存在", this.String())
	}

	return nil
}

// 添加主键
func (this *Table) addPK(dao *dao.DefaultDao) error {
	// 获取 主键
	pkColumnNames, err := dao.FindTablePKColumnNames(this.GetSchema(), this.TableName)
	if err != nil {
		return fmt.Errorf("获取主键字段名出错. %v", err)
	}
	if len(pkColumnNames) > 0 {
		this.PKColumnNames = pkColumnNames
		this.PKType = PKTypePK
		return nil
	}
	log.Warnf("表: %s 没有主键", this.String())

	// 获取唯一键做 主键
	ukColumnNames, ukName, err := dao.FindTableUKColumnNames(this.GetSchema(), this.TableName)
	if err != nil {
		return fmt.Errorf("获取唯一键做主键失败. %v", err)
	}
	if len(ukColumnNames) > 0 {
		log.Warnf("表: %s 设置唯一键 %s 当作主键", this.String(), ukName)
		this.PKColumnNames = ukColumnNames
		this.PKType = PKTypePK
		return nil
	}
	log.Warnf("表: %s 没有唯一键", this.String())

	// 所有字段为 主键
	this.PKColumnNames = this.ColumnNames
	this.PKType = PKTypeAllColumns
	log.Warnf("表: %s 所有字段作为该表的唯一键", this.String())

	return nil
}

// 初始每个字段的位置
func (this *Table) initColumnPos() {
	columnPos := make(map[string]int)
	for pos, name := range this.ColumnNames {
		columnPos[name] = pos
	}
	this.ColumnPos = columnPos
}

// 初始化sql模板
func (this *Table) initSQLTemplate() {
	this.initInsertTemplate()
	this.initUpdateTemplate()
	this.initDeleteTemplate()
}

// 初始化 insert sql 模板
func (this *Table) initInsertTemplate() {
	template := "INSERT INTO `%s`.`%s`(`%s`) VALUES"
	this.InsertTemplate = fmt.Sprintf(template, this.GetSchema(), this.TableName,
		strings.Join(this.ColumnNames, "`, `"))
	this.InsertValuePlaceholderTemplate = fmt.Sprintf("(%s)",
		utils.StrRepeat("%#v", len(this.ColumnNames), ","))
}

// 初始化 update sql 模板
func (this *Table) initUpdateTemplate() {
	template := "UPDATE `%s`.`%s` SET %s WHERE %s;\n"
	this.UpdateTemplate = fmt.Sprintf(template, this.GetSchema(), this.TableName,
		utils.SqlExprPlaceholderByColumns(this.ColumnNames, "=", "%#v", ", "),
		utils.SqlExprPlaceholderByColumns(this.PKColumnNames, "=", "%#v", "AND "))
}

// 初始化 delete sql 模板
func (this *Table) initDeleteTemplate() {
	template := "DELETE FROM `%s`.`%s` WHERE %s;\n"
	this.DeleteTemplate = fmt.Sprintf(template, this.GetSchema(), this.TableName,
		utils.SqlExprPlaceholderByColumns(this.PKColumnNames, "=", "%#v", "AND "))
}

func (this *Table) SetPKValues(row []interface{}, pkValues []interface{}) {
	for i, v := range this.PKColumnNames {
		pkValues[i] = row[this.ColumnPos[v]]
	}
}
