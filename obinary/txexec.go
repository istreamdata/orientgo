package obinary

import (
	"fmt"

	"github.com/quux00/ogonori/oschema"
)

type operationType byte

const (
	UpdateOp operationType = 1
	DeleteOp operationType = 2
	CreateOp operationType = 3
)

type TxEntry struct {
	Optype operationType
	Doc    *oschema.ODocument
}

func (txe TxEntry) String() string {
	return fmt.Sprintf("TxEntry<Op: %d; DocRID: %s>", txe.Optype, txe.Doc.RID.String())
}

func ExecTransaction(txentries []TxEntry) error {
	return nil
}
