package orient

import "math/big"

type Decimal struct { // TODO: use big.Float for Go 1.5
	Scale int
	Value *big.Int
}
