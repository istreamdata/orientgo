package osql

import "gopkg.in/istreamdata/orientgo.v1/oerror"

//
// ogonoriTx implements the database/sql/driver.Tx interface.
//
type ogonoriTx struct {
	conn *ogonoriConn
}

//
// Note: Transactions in OrientDB are implemented heavily on the client side
// and that will take some time to work out - so these are not implemented yet
//

func (tx *ogonoriTx) Commit() error {
	if tx.conn == nil {
		return oerror.ErrInvalidConn{Msg: "ogonoriConn not initialized in ogonoriTx#Commit"}
	}

	return nil
}

func (tx *ogonoriTx) Rollback() error {
	if tx.conn == nil {
		return oerror.ErrInvalidConn{Msg: "ogonoriConn not initialized in ogonoriTx#Rollback"}
	}

	return nil
}

// type Tx interface {
//         Commit() error
//         Rollback() error
// }
