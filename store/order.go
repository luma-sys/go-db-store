package store

type Order string

const (
	OrderAsc  Order = "ASC"
	OrderDesc Order = "Desc"
)

func (s Order) IsValid() bool {
	switch s {
	case OrderAsc, OrderDesc:
		return true
	}

	return false
}
