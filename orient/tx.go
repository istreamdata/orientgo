package orient // TODO: change to ogonori and/or move to top-level

import (
	"github.com/dyy18/orientgo/obinary"
	"github.com/dyy18/orientgo/oschema"
)

/* ---[ types ]--- */

// TODO: should this be an interface?
type Tx struct {
	dbc       *obinary.DBClient
	txentries []obinary.TxEntry
}

type InvalidTxState struct {
	msg string
}

func (itx InvalidTxState) Error() string {
	return itx.msg
}

/* ---[ constructor ]--- */

func TxBegin(dbc *obinary.DBClient) *Tx {
	return &Tx{dbc: dbc, txentries: make([]obinary.TxEntry, 0, 8)}
}

/* ---[ methods ]--- */

func (tx *Tx) Update(doc *oschema.ODocument, docs ...*oschema.ODocument) {
	tx.txentries = append(tx.txentries, obinary.TxEntry{Optype: obinary.UpdateOp, Doc: doc})

	for _, xdoc := range docs {
		tx.txentries = append(tx.txentries, obinary.TxEntry{Optype: obinary.UpdateOp, Doc: xdoc})
	}
}

func (tx *Tx) Delete(doc *oschema.ODocument, docs ...*oschema.ODocument) {
	tx.txentries = append(tx.txentries, obinary.TxEntry{Optype: obinary.DeleteOp, Doc: doc})

	for _, xdoc := range docs {
		tx.txentries = append(tx.txentries, obinary.TxEntry{Optype: obinary.DeleteOp, Doc: xdoc})
	}
}

func (tx *Tx) Create(doc *oschema.ODocument, docs ...*oschema.ODocument) {
	tx.txentries = append(tx.txentries, obinary.TxEntry{Optype: obinary.CreateOp, Doc: doc})

	for _, xdoc := range docs {
		tx.txentries = append(tx.txentries, obinary.TxEntry{Optype: obinary.CreateOp, Doc: xdoc})
	}
}

func (tx *Tx) Commit() error {
	if tx.dbc == nil {
		return InvalidTxState{msg: "Transaction does not have a valid associated DBClient"}
	}

	err := obinary.ExecTransaction(tx.txentries)

	// dbc.Tx = nil  // TODO: mark the dbc as no longer in a tx
	return err
}

func (tx *Tx) Rollback() error {
	// dbc.Tx = nil  // TODO: mark the dbc as no longer in a tx
	tx.dbc = nil
	tx.txentries = []obinary.TxEntry{}

	return nil
}
