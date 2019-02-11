package manal

import (
	"bytes"
	"fmt"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/haqi/config"
	"github.com/daiguadaidai/haqi/models"
	"github.com/daiguadaidai/haqi/schema"
	"github.com/siddontang/go-mysql/replication"
)

type MComsume struct {
	TMC           *config.ToMySQLConfig
	TDBC          *config.DBConfig
	CurrPosition  *models.Position
	EventChan     chan *EventData
	TransTableMap map[string]*schema.Table
	Success       bool
}

func NewMComsume(tmc *config.ToMySQLConfig, tdbc *config.DBConfig) *MComsume {
	return &MComsume{
		CurrPosition: new(models.Position),
		TMC:          tmc,
		TDBC:         tdbc,
	}
}

func (this *MComsume) Comsume() error {
	for ev := range this.EventChan {
		switch e := ev.BinlogEvent.Event.(type) {
		case *replication.RowsEvent:
			key := fmt.Sprintf("%s.%s", string(e.Table.Schema), string(e.Table.Table))
			t, ok := this.TransTableMap[key]
			if !ok {
				seelog.Error("没有获取到表需要回滚的表信息(生成原sql数据的时候) %s.", key)
				continue
			}
			switch ev.BinlogEvent.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				if err := this.writeInsert(e, t); err != nil {
					return err
				}
			}
		}
	}

	this.Success = true
	return nil
}

func (this *MComsume) writeInsert(ev *replication.RowsEvent, tbl *schema.Table) error {
	var buf bytes.Buffer
	for i, row := range ev.Rows {
		if i == 0 {
			buf.WriteString(tbl.InsertTemplate)
		} else {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf(tbl.InsertValuePlaceholderTemplate, row...))
	}

	fmt.Println(buf.String())
	return nil
}