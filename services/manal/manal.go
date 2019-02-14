package manal

import (
	"context"
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/haqi/config"
	"github.com/daiguadaidai/haqi/models"
	"github.com/daiguadaidai/haqi/schema"
	"github.com/daiguadaidai/haqi/services/types"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"sync"
	"time"
)

type TransType int8

const (
	TransTypeNone TransType = iota
	TransTypeAll
	TransTypePartial
)

type EventData struct {
	LogFile     string
	LogPos      uint32
	BinlogEvent *replication.BinlogEvent
}

// mysql binlog generator
type Manal struct {
	ctx               context.Context
	cancel            context.CancelFunc
	ProductSuccess    bool
	Syncer            *replication.BinlogSyncer
	EventChan         chan *EventData
	EventChanIsClosed bool
	sync.Mutex
	TMC             *config.ToMySQLConfig
	ODBC            *config.DBConfig
	TDBC            *config.DBConfig
	CurrentTable    *models.DBTable // 但前的表
	StartPosition   *models.Position
	EndPosition     *models.Position
	CurrentPosition *models.Position
	CurrentThreadID uint32
	TransTableMap   map[string]*schema.Table
	TransType
	MComsume *MComsume
}

func NewManal(tmc *config.ToMySQLConfig, odbc *config.DBConfig, tdbc *config.DBConfig) (*Manal, error) {
	var err error
	manal := new(Manal)
	// 设置配置文件
	manal.TMC = tmc
	manal.ODBC = odbc // 源数据库配置信息
	manal.TDBC = tdbc // 目标数据库配置信息

	// 设置其他参数
	manal.ctx, manal.cancel = context.WithCancel(context.Background())
	manal.CurrentTable = new(models.DBTable)
	manal.CurrentPosition = new(models.Position)
	manal.EventChan = make(chan *EventData, 1000)
	manal.TransTableMap = make(map[string]*schema.Table)
	// 开始位点
	manal.StartPosition, err = GetStartPosition(&tmc.BaseConfig, odbc)
	if err != nil {
		return nil, err
	}
	// 获取结束位点
	manal.EndPosition = GetEndPosition(&tmc.BaseConfig)

	// 获取需要执行的表
	transTables, transType, err := FindTransTables(&tmc.BaseConfig, odbc)
	if err != nil {
		return nil, err
	}
	manal.TransType = transType
	if transType == TransTypePartial {
		for _, table := range transTables {
			if err = manal.cacheTransTable(table.TableSchema, table.TableName); err != nil {
				return nil, err
			}
		}
	}

	// 设置消费者信息
	manal.MComsume = NewMComsume(tmc, tdbc)
	manal.MComsume.EventChan = manal.EventChan
	manal.MComsume.TransTableMap = manal.TransTableMap

	// 设置获取 sync
	cfg := odbc.GetSyncerConfig()
	manal.Syncer = replication.NewBinlogSyncer(cfg)

	return manal, nil
}

// 保存需要进行rollback的表
func (this *Manal) cacheTransTable(sName string, tName string) error {
	// 比较和修复目标表结构
	if err := CompareAndRePairTable(this.ODBC, this.TDBC, sName, this.TMC.SchemaSuffix, tName); err != nil {
		return err
	}

	// 获取表信息
	key := fmt.Sprintf("%s.%s", sName, tName)
	t, err := schema.NewTable(sName, this.TMC.SchemaSuffix, tName, this.ODBC.Host, this.ODBC.Port)
	if err != nil {
		return err
	}

	this.TransTableMap[key] = t

	return nil
}

func (this *Manal) emit() error {
	defer this.stopProduct()
	defer this.Syncer.Close()

	pos := mysql.Position{this.StartPosition.File, this.StartPosition.Position}
	streamer, err := this.Syncer.StartSync(pos)
	if err != nil {
		return err
	}
	for { // 遍历event获取第二个可用的时间戳
		select {
		case <-this.ctx.Done():
			seelog.Info("终止发射binlog event")
			return nil
		default:
			ev, err := streamer.GetEvent(context.Background())
			if err != nil {
				return err
			}
			if isStop, err := this.handleEvent(ev); err != nil {
				return err
			} else if isStop {
				return nil
			}
		}
	}

	return nil
}

// 处理binlog事件
func (this *Manal) handleEvent(ev *replication.BinlogEvent) (bool, error) {
	this.CurrentPosition.Position = ev.Header.LogPos // 设置当前位点

	// 判断是否到达了结束位点
	if ok := this.rlEndPos(); ok {
		seelog.Infof("解析的位点 %s 已经超过执行的位点 %s",
			this.CurrentPosition.String(), this.EndPosition.String())
		return true, nil
	}

	switch e := ev.Event.(type) {
	case *replication.RotateEvent:
		this.CurrentPosition.File = string(e.NextLogName)
		// 判断是否到达了结束位点
		if ok := this.rlEndPos(); ok {
			seelog.Infof("(in RotateEvent)解析的位点 %s 已经超过执行的位点 %s",
				this.CurrentPosition.String(), this.EndPosition.String())
			return true, nil
		}
	case *replication.QueryEvent:
		this.CurrentThreadID = e.SlaveProxyID
	case *replication.TableMapEvent:
		this.handleMapEvent(e)
	case *replication.RowsEvent:
		if err := this.produceRowEvent(ev); err != nil {
			return true, err
		}
	}

	return false, nil
}

func (this *Manal) rlEndPos() bool {
	// 判断是否超过了指定位点
	if len(this.EndPosition.File) != 0 {
		if this.EndPosition.LessThan(this.CurrentPosition) {
			this.ProductSuccess = true // 代表任务完成
			return true
		}
	}

	return false
}

// 处理 TableMapEvent
func (this *Manal) handleMapEvent(ev *replication.TableMapEvent) error {
	this.CurrentTable.TableSchema = string(ev.Schema)
	this.CurrentTable.TableName = string(ev.Table)

	// 判断是否所有的表都要进行传输 并且缓存没有缓存的表
	if this.TransType == TransTypeAll {
		if _, ok := this.TransTableMap[this.CurrentTable.String()]; !ok {
			if err := this.cacheTransTable(this.CurrentTable.TableSchema, this.CurrentTable.TableName); err != nil {
				return err
			}
		}
	}
	return nil
}

// 产生事件
func (this *Manal) produceRowEvent(ev *replication.BinlogEvent) error {
	// 判断是否是指定的 thread id
	if this.TMC.ThreadID != 0 && this.TMC.ThreadID != this.CurrentThreadID {
		//  没有指定, 指定了 thread id, 但是 event thread id 不等于 指定的 thread id
		return nil
	}

	// 判断是否是有过滤相关的event类型
	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		if !this.TMC.EnableTransInsert {
			return nil
		}
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		if !this.TMC.EnableTransUpdate {
			return nil
		}
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		if !this.TMC.EnableTransDelete {
			return nil
		}
	}

	// 判断是否指定表要执行, 还是所有表要执行
	switch e := ev.Event.(type) {
	case *replication.RowsEvent:
		this.CurrentTable.TableSchema = string(e.Table.Schema)
		this.CurrentTable.TableName = string(e.Table.Table)

		if this.TransType == TransTypePartial { // 部分表
			if _, ok := this.TransTableMap[this.CurrentTable.String()]; !ok {
				return nil
			}
		} else { // 所有的表需要执行
			if _, ok := this.TransTableMap[this.CurrentTable.String()]; !ok {
				if err := this.cacheTransTable(this.CurrentTable.TableSchema, this.CurrentTable.TableName); err != nil {
					return err
				}
			}
		}
		this.EventChan <- &EventData{
			LogFile:     this.CurrentPosition.File,
			LogPos:      this.CurrentPosition.Position,
			BinlogEvent: ev,
		}
	default:
		return fmt.Errorf("未匹配的 RowsEvent.")
	}

	return nil
}

func (this *Manal) stopProduct() {
	this.cancel()
}

func (this *Manal) closeChan() {
	this.Lock()
	defer this.Unlock()
	if !this.EventChanIsClosed {
		close(this.EventChan)
	}
}

func (this *Manal) Start() error {
	wg := new(sync.WaitGroup)

	wg.Add(1)
	go this.product(wg)

	wg.Add(1)
	go this.comsume(wg)

	wg.Add(1)
	go this.readAPI(wg)

	wg.Add(1)
	go this.updateAPI(wg)

	wg.Wait()

	if !this.ProductSuccess {
		return fmt.Errorf("binlog没有产生完成. binlog解析到 %s, 应用到位点: %s, 结束位点为 %s",
			this.CurrentPosition.String(), this.MComsume.CurrPosition.String(), this.EndPosition.String())
	}
	if !this.MComsume.Success {
		return fmt.Errorf("binlog没有应用完成. binlog解析到 %s, 应用到位点: %s, 结束位点为 %s",
			this.CurrentPosition.String(), this.MComsume.CurrPosition.String(), this.EndPosition.String())
	}

	return nil
}

func (this *Manal) product(wg *sync.WaitGroup) {
	defer wg.Done()
	if err := this.emit(); err != nil {
		seelog.Error(err.Error())
	}

	close(this.EventChan)
}

func (this *Manal) comsume(wg *sync.WaitGroup) {
	defer wg.Done()
	defer func() {
		this.MComsume.IsQuit = true
	}()

	if err := this.MComsume.Comsume(); err != nil {
		this.stopProduct()
		seelog.Error(err.Error())
		return
	}
}

func (this *Manal) readAPI(wg *sync.WaitGroup) {
	defer wg.Done()

	if !this.TMC.EnableReadAPI() {
		seelog.Warnf("没有指定读取API. 本任务将不使用API读取结束位点信息")
		return
	}

	ticker := time.NewTicker(15 * time.Second)
	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case <-ticker.C:
			// 读取API信息
			readInfo, err := types.GetReadInfo(this.TMC.TaskUUID, this.TMC.ReadAPI)
			if err != nil {
				seelog.Errorf("获取API信息失败, 无法获取到结束位点. %v", err)
				continue
			}
			if len(readInfo.EndLogFile) == 0 {
				seelog.Warnf("API获取到不正确的位点信息 %s:%d", readInfo.EndLogFile, readInfo.EndLogPos)
				continue
			}
			// 将获取的结束位点信息赋值给当前任务
			// 获取的信息和指定的相等不进行赋值
			if readInfo.EndLogFile == this.EndPosition.File && readInfo.EndLogPos == this.EndPosition.Position {
				continue
			}
			// 不相等进行赋值
			this.EndPosition.File = readInfo.EndLogFile
			this.EndPosition.Position = readInfo.EndLogPos
		case <-this.ctx.Done():
			seelog.Info("停止读取API信息")
			return
		default:
		}
	}
}

func (this *Manal) updateAPI(wg *sync.WaitGroup) {
	defer wg.Done()

	if !this.TMC.EnableUpdateAPI() {
		seelog.Warnf("没有指定更新API. 本任务不会对相关信息进行更新")
		return
	}

	ticker1 := time.NewTicker(15 * time.Second)
	ticker2 := time.NewTicker(1 * time.Second)
	defer func() {
		ticker1.Stop()
		ticker2.Stop()
	}()

	for {
		select {
		case <-ticker1.C:
			saveInfo := &types.SaveInfo{
				ParseLogFile: this.CurrentPosition.File,
				ParseLogPos:  this.CurrentPosition.Position,
				ApplyLogFile: this.MComsume.CurrPosition.File,
				ApplyLogPos:  this.MComsume.CurrPosition.Position,
				EndLogFile:   this.EndPosition.File,
				EndLogPos:    this.EndPosition.Position,
			}
			if err := types.UpdateSaveInfo(this.TMC.TaskUUID, this.TMC.UpdateAPI, saveInfo); err != nil {
				seelog.Errorf("保存信息出错. %v", err)
			}
			// 更新信息
		case <-ticker2.C:
			if this.MComsume.IsQuit {
				// 信息信息
				seelog.Info("停止更新接口")
				saveInfo := &types.SaveInfo{
					ParseLogFile: this.CurrentPosition.File,
					ParseLogPos:  this.CurrentPosition.Position,
					ApplyLogFile: this.MComsume.CurrPosition.File,
					ApplyLogPos:  this.MComsume.CurrPosition.Position,
					EndLogFile:   this.EndPosition.File,
					EndLogPos:    this.EndPosition.Position,
				}
				if err := types.UpdateSaveInfo(this.TMC.TaskUUID, this.TMC.UpdateAPI, saveInfo); err != nil {
					seelog.Errorf("保存信息出错. %v", err)
				}
				return
			}
		default:
		}
	}
}
