package orient

import "math/big"

func isDecimal(o interface{}) bool {
	switch o.(type) {
	case *big.Int, Decimal:
		return true
	default:
		return false
	}
}

type Decimal struct {
	Scale int
	Value *big.Int
}
