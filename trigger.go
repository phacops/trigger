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
	Name string
	Type int
	From string
	To   string
}

func New(name, fromTable, toTable string, triggerType int) (*Trigger, error) {
	if name == "" || fromTable == "" || toTable == "" {
		return nil, fmt.Errorf("no empty value allowed")
	}

	if triggerType < InsertTriggerType || triggerType > DeleteTriggerType {
		return nil, fmt.Errorf("unsupported trigger type")
	}

	return &Trigger{
		Name: name,
		Type: triggerType,
		From: fromTable,
		To:   toTable,
	}, nil
}

func fieldsFromTable(tx *sql.Tx, table string) (fields []string, err error) {
	rows, err := tx.Query("show columns from `" + table + "`")

	if err != nil {
		return
	}

	var fieldName, fieldType, fieldNull, fieldKey, fieldExtra string
	var fieldDefault sql.NullString

	for rows.Next() {
		err = rows.Scan(&fieldName, &fieldType, &fieldNull, &fieldKey, &fieldDefault, &fieldExtra)

		if err != nil {
			return
		}

		fields = append(fields, fieldName)
	}

	return
}

func (t *Trigger) Create(tx *sql.Tx, dropIfExists bool) (err error) {
	if tx == nil {
		return fmt.Errorf("a valid transaction is needed")
	}

	if dropIfExists {
		err = t.Drop(tx)

		if err != nil {
			return
		}
	}

	if t.Type >= len(SqlRequests) {
		return fmt.Errorf("trigger type not supported")
	}

	fields, err := fieldsFromTable(tx, t.To)

	if err != nil {
		return
	}

	var sql string

	if t.Type != DeleteTriggerType {
		sql = fmt.Sprintf(SqlRequests[t.Type], t.Name, t.From, t.To, strings.Join(fields, "`, `"), strings.Join(fields, "`, NEW.`"))
	} else {
		sql = fmt.Sprintf(SqlRequests[t.Type], t.Name, t.From, t.To, t.To)
	}

	_, err = tx.Exec(sql)

	return
}

func (t *Trigger) Drop(tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("a valid transaction is needed")
	}

	var schema string

	err := tx.QueryRow("select database()").Scan(&schema)

	if err != nil {
		return err
	}

	_, err = tx.Exec(fmt.Sprintf("drop trigger if exists `%s`.`%s`", schema, t.Name))

	return err
}
