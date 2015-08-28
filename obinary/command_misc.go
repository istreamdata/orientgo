package obinary

import (
	"fmt"
)

var (
	ErrNoNodesReturned       = fmt.Errorf("No nodes were returned from the database while expecting one")
	ErrMultipleNodesReturned = fmt.Errorf("Multiple nodes were returned from the database while expecting one")
)

type ErrUnexpectedResultCount struct {
	Expected int
	Count    int
}

func (e ErrUnexpectedResultCount) Error() string {
	return fmt.Sprintf("expected %d record to be modified, but got %d", e.Expected, e.Count)
}

func checkExpected(expected int) func(Records, error) error {
	return func(recs Records, err error) error {
		if err != nil {
			return err
		}
		var mod int
		if err = recs.DeserializeAll(&mod); err != nil {
			return err
		}
		if expected >= 0 && expected != mod {
			err = ErrUnexpectedResultCount{Expected: expected, Count: mod}
		} else if expected < 0 && mod == 0 {
			err = ErrUnexpectedResultCount{Expected: expected, Count: mod}
		}
		return err
	}
}

func (dbc *Client) SQLCommandExpect(expected int, sql string, params ...interface{}) error {
	return checkExpected(expected)(dbc.SQLCommand(nil, sql, params...))
}

func (dbc *Client) SQLBatch(result interface{}, sql string, params ...interface{}) (Records, error) {
	return dbc.ExecScript(result, LangSQL, sql, params...)
}

func (dbc *Client) SQLBatchExpect(expected int, sql string, params ...interface{}) error {
	return checkExpected(expected)(dbc.SQLBatch(nil, sql, params...))
}

func (dbc *Client) SQLQueryOne(result interface{}, sql string, params ...interface{}) (Record, error) {
	recs, err := dbc.SQLQuery(result, nil, sql, params...)
	if err != nil {
		return nil, err
	}
	return recs.One()
}

func (dbc *Client) SQLCommandOne(result interface{}, sql string, params ...interface{}) (Record, error) {
	recs, err := dbc.SQLCommand(result, sql, params...)
	if err != nil {
		return nil, err
	}
	return recs.One()
}

func (dbc *Client) SQLBatchOne(result interface{}, sql string, params ...interface{}) (Record, error) {
	recs, err := dbc.ExecScript(result, LangSQL, sql, params...)
	if err != nil {
		return nil, err
	}
	return recs.One()
}
