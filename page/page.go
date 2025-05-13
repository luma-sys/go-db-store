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

type Queryable interface {
	Initialize(f ...map[string]any) // initializes the queryable
	GetFilter() map[string]any      // returns the filter
}

type PaginationQueryable interface {
	Queryable
	GetPage() int64     // returns the page number
	GetLimit() int64    // returns the limit
	GetSortBy() string  // returns the sort by
	GetOrderBy() string // returns the order by
}

type FilterQuery struct {
	Filter map[string]any // Filtros para a busca
}

func (q *FilterQuery) Initialize(f ...map[string]any) {
	if q.Filter == nil && len(f) > 0 {
		q.Filter = f[0]
	}
}

func (q *FilterQuery) GetFilter() map[string]any {
	return q.Filter
}

type PaginationQuery struct {
	Page    int64          // número da pagina
	Limit   int64          // quantidade de itens por pagina
	SortBy  string         // nome da propriedade
	OrderBy string         // "asc" ou "desc"
	Filter  map[string]any // Filtros para a busca
}

func (q *PaginationQuery) Initialize(f ...map[string]any) {
	if q.Page < 1 {
		q.Page = 1
	}
	if q.Limit <= 0 {
		q.Limit = 10
	}
	if q.SortBy == "" {
		q.SortBy = "createdAt"
	}
	if q.OrderBy == "" {
		q.OrderBy = "ASC"
	}
	if q.Filter == nil && len(f) > 0 {
		q.Filter = f[0]
	}
}

func (q *PaginationQuery) GetFilter() map[string]any {
	return q.Filter
}

func (q *PaginationQuery) GetPage() int64 {
	return q.Page
}

func (q *PaginationQuery) GetLimit() int64 {
	return q.Limit
}

func (q *PaginationQuery) GetSortBy() string {
	return q.SortBy
}

func (q *PaginationQuery) GetOrderBy() string {
	return q.OrderBy
}
