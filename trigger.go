package trigger

import (
	"database/sql"
	"fmt"
	"strings"
)

const (
	InsertTriggerType = iota
	UpdateTriggerType
	DeleteTriggerType
)

var (
	SqlRequests = []string{
		"create trigger `%s` after insert on `%s` for each row replace into `%s` (`%s`) values (NEW.`%s`)",
		"create trigger `%s` after update on `%s` for each row replace into `%s` (`%s`) values (NEW.`%s`)",
		"create trigger `%s` after delete on `%s` for each row delete ignore from `%s` where `%s`.`id` = OLD.`id`",
	}
)

type Trigger struct {
	Schema string
	Name   string
	Type   int
	From   string
	To     string
	Fields []string
	Tx     *sql.Tx
}

func New(schema, name, fromTable, toTable string, triggerType int, fields []string, tx *sql.Tx) (*Trigger, error) {
	if schema == "" || name == "" || fromTable == "" || toTable == "" || len(fields) == 0 {
		return nil, fmt.Errorf("no empty value allowed")
	}

	if triggerType < InsertTriggerType || triggerType > DeleteTriggerType {
		return nil, fmt.Errorf("unsupported trigger type")
	}

	if tx == nil {
		return nil, fmt.Errorf("a valid transaction is needed")
	}

	return &Trigger{
		Schema: schema,
		Name:   name,
		Type:   triggerType,
		From:   fromTable,
		To:     toTable,
		Fields: fields,
		Tx:     tx,
	}, nil
}

func (t *Trigger) Create(dropIfExists bool) (err error) {
	if dropIfExists {
		err = t.Drop()

		if err != nil {
			return
		}
	}

	if t.Type >= len(SqlRequests) {
		return fmt.Errorf("trigger type not supported")
	}

	var sql string

	if t.Type == DeleteTriggerType {
		sql = fmt.Sprintf(SqlRequests[t.Type], t.Name, t.From, t.To, strings.Join(t.Fields, "`, NEW.`"))
	} else {
		sql = fmt.Sprintf(SqlRequests[t.Type], t.Name, t.From, t.To, t.To)
	}

	_, err = t.Tx.Exec(sql)

	return
}

func (t *Trigger) Drop() error {
	_, err := t.Tx.Exec(fmt.Sprintf("drop trigger if exists `%s`.`%s`", t.Schema, t.Name))

	return err
}
