package osql

//
// ogonoriResult implements the sql/driver.Result inteface.
//
type ogonoriResult struct {
	affectedRows int64
	insertId     int64
}

//
// LastInsertId returns the database's auto-generated ID after,
// for example, an INSERT into a table with primary key.
//
func (res *ogonoriResult) LastInsertId() (int64, error) {
	return res.insertId, nil
}

//
// RowsAffected returns the number of rows affected by the query.
//
func (res *ogonoriResult) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}
