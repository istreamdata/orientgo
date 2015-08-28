package obinary

import (
	"encoding/json"
	"fmt"
	"strings"
)

func sqlEscape(s string) string { // TODO: escape things in a right way
	s = strings.Replace(s, `\`, `\\`, -1)
	s = strings.Replace(s, `"`, `\"`, -1)
	return `"` + s + `"`
}

func (dbc *Client) CreateScriptFunc(fnc Function) error {
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

func (dbc *Client) CallScriptFunc(result interface{}, name string, params ...interface{}) (Records, error) {
	sparams := make([]string, 0, len(params))
	for _, p := range params {
		data, _ := json.Marshal(p)
		sparams = append(sparams, string(data))
	}
	return dbc.ExecScript(result, LangJS, name+`(`+strings.Join(sparams, ",")+`)`)
}

// CallScriptFuncJSON is a workaround for driver bug. It allow to return pure JS objects from DB functions.
func (dbc *Client) CallScriptFuncJSON(result interface{}, name string, params ...interface{}) error {
	sparams := make([]string, 0, len(params))
	for _, p := range params {
		data, _ := json.Marshal(p)
		sparams = append(sparams, string(data))
	}
	var jsonData string
	_, err := dbc.ExecScript(&jsonData, LangJS, `JSON.stringify(`+name+`(`+strings.Join(sparams, ",")+`))`)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(jsonData), result)
}

type Function struct {
	Name   string
	Lang   ScriptLang
	Params []string
	Idemp  bool // is idempotent
	Code   string
}

func (dbc *Client) InitScriptFunc(fncs ...Function) (err error) {
	for _, fnc := range fncs {
		if fnc.Lang == "" {
			err = fmt.Errorf("no language provided for function '%s'", fnc.Name)
			return
		}
		dbc.DeleteScriptFunc(fnc.Name)
		err = dbc.CreateScriptFunc(fnc)
		if err != nil && !strings.Contains(err.Error(), "found duplicated key") {
			return
		}
	}
	return nil
}
