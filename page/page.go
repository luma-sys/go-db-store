package page

import "math"

type PageMeta struct {
	TotalItems   int `json:"totalItems"`
	CurrentPage  int `json:"currentPage"`
	ItemsPerPage int `json:"itemsPerPage"`
	ItemCount    int `json:"itemCount"`
	TotalPages   int `json:"totalPages"`
}

type Page[T any] struct {
	Items []T      `json:"items"`
	Meta  PageMeta `json:"meta"`
}

type Cursor[T any] struct {
	Next  *string `json:"next"`
	Items []T     `json:"items"`
}

func New[T any](items []T, page, limit, totalItems int64) *Page[T] {
	if items == nil {
		items = []T{}
	}

	totalPages := 0
	if len(items) > 0 {
		totalPages = int(math.Ceil(float64(totalItems) / float64(limit)))
	}

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

func NewEmptyCursor[T any]() *Cursor[T] {
	return &Cursor[T]{
		Items: []T{},
		Next:  nil,
	}
}

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

func Skip(page, limit int64) int64 {
	return (page - 1) * limit
}
