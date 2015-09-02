package obinary

import (
	"encoding/json"
	"fmt"
	"github.com/dyy18/orientgo"
	"strings"
)

func sqlEscape(s string) string { // TODO: escape things in a right way
	s = strings.Replace(s, `\`, `\\`, -1)
	s = strings.Replace(s, `"`, `\"`, -1)
	return `"` + s + `"`
}

func (dbc *Client) CreateScriptFunc(fnc orient.Function) error {
	sql := `CREATE FUNCTION ` + fnc.Name + ` ` + sqlEscape(fnc.Code)
	if len(fnc.Params) > 0 {
		sql += ` PARAMETERS [` + strings.Join(fnc.Params, ", ") + `]`
	}
	sql += ` IDEMPOTENT ` + fmt.Sprint(fnc.Idemp)
	if fnc.Lang != "" {
		sql += ` LANGUAGE ` + string(fnc.Lang)
	}
	_, err := dbc.SQLCommand(nil, sql)
	return err
}

func (dbc *Client) DeleteScriptFunc(name string) error {
	_, err := dbc.SQLCommand(nil, `DELETE FROM OFunction WHERE name = ?`, name)
	return err
}

func (dbc *Client) UpdateScriptFunc(name string, script string) error {
	_, err := dbc.SQLCommand(nil, `UPDATE OFunction SET code = ? WHERE name = ?`, script, name)
	return err
}

func (dbc *Client) CallScriptFunc(result interface{}, name string, params ...interface{}) (orient.Records, error) {
	sparams := make([]string, 0, len(params))
	for _, p := range params {
		data, _ := json.Marshal(p)
		sparams = append(sparams, string(data))
	}
	return dbc.ExecScript(result, orient.LangJS, name+`(`+strings.Join(sparams, ",")+`)`)
}
