package page

import "math"

// PageMeta representa um metadata de paginação
type PageMeta struct {
	TotalItems   int `json:"totalItems"`
	CurrentPage  int `json:"currentPage"`
	ItemsPerPage int `json:"itemsPerPage"`
	ItemCount    int `json:"itemCount"`
	TotalPages   int `json:"totalPages"`
}

// Page representa uma paginação
type Page[T any] struct {
	Items []T      `json:"items"`
	Meta  PageMeta `json:"meta"`
}

// Cursor representa um cursor de paginação [OLD MODE]
type Cursor[T any] struct {
	Next  *string `json:"next"`
	Items []T     `json:"items"`
}

// New cria uma paginação
func New[T any](items []T, page, limit, totalItems int64) *Page[T] {
	if items == nil {
		items = []T{}
	}

	totalPages := CalculateTotalPages(totalItems, limit)

	meta := PageMeta{
		TotalItems:   int(totalItems),
		CurrentPage:  int(page),
		ItemsPerPage: int(limit),
		ItemCount:    len(items),
		TotalPages:   totalPages,
	}

	return &Page[T]{
		Items: items,
		Meta:  meta,
	}
}

// NewEmpty cria uma paginação vazia
func NewEmpty[T any](page, limit int64) *Page[T] {
	return &Page[T]{
		Items: []T{},
		Meta: PageMeta{
			TotalItems:   0,
			CurrentPage:  int(page),
			ItemsPerPage: int(limit),
			ItemCount:    0,
			TotalPages:   1,
		},
	}
}

// NewEmptyCursor cria um cursor vazio [OLD MODE]
func NewEmptyCursor[T any]() *Cursor[T] {
	return &Cursor[T]{
		Items: []T{},
		Next:  nil,
	}
}

// NewEmptyCursor cria um cursor [OLD MODE]
func NewCursor[T any](items []T, limit int64, next func(item T) string) *Cursor[T] {
	if items == nil {
		items = []T{}
	}

	if len(items) > int(limit) {
		cursor := next(items[limit])
		return &Cursor[T]{
			Items: items[:limit],
			Next:  &cursor,
		}
	}

	return &Cursor[T]{
		Items: items,
		Next:  nil,
	}
}

// Skip retorna o offset para a paginação
func Skip(page, limit int64) int64 {
	return (page - 1) * limit
}

func CalculateTotalPages(count, limit int64) int {
	if limit == 0 {
		return 0
	}

	return int(math.Ceil(float64(count) / float64(limit)))
}
