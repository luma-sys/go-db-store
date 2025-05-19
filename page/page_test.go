package page

import (
	"testing"
)

type TestItem struct {
	ID   int
	Name string
}

func TestNew(t *testing.T) {
	tests := []struct {
		name       string
		items      []TestItem
		page       int64
		limit      int64
		totalItems int64
		want       *Page[TestItem]
	}{
		{
			name: "deve criar uma paginação com itens",
			items: []TestItem{
				{ID: 1, Name: "Item 1"},
				{ID: 2, Name: "Item 2"},
			},
			page:       1,
			limit:      10,
			totalItems: 2,
			want: &Page[TestItem]{
				Items: []TestItem{
					{ID: 1, Name: "Item 1"},
					{ID: 2, Name: "Item 2"},
				},
				Meta: PageMeta{
					TotalItems:   2,
					CurrentPage:  1,
					ItemsPerPage: 10,
					ItemCount:    2,
					TotalPages:   1,
				},
			},
		},
		{
			name:       "deve criar uma paginação com items nil",
			items:      nil,
			page:       1,
			limit:      10,
			totalItems: 0,
			want: &Page[TestItem]{
				Items: []TestItem{},
				Meta: PageMeta{
					TotalItems:   0,
					CurrentPage:  1,
					ItemsPerPage: 10,
					ItemCount:    0,
					TotalPages:   0,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := New(tt.items, tt.page, tt.limit, tt.totalItems)
			if !comparePage(got, tt.want) {
				t.Errorf("New() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewEmpty(t *testing.T) {
	got := NewEmpty[TestItem](1, 10)
	want := &Page[TestItem]{
		Items: []TestItem{},
		Meta: PageMeta{
			TotalItems:   0,
			CurrentPage:  1,
			ItemsPerPage: 10,
			ItemCount:    0,
			TotalPages:   1,
		},
	}

	if !comparePage(got, want) {
		t.Errorf("NewEmpty() = %v, want %v", got, want)
	}
}

func TestSkip(t *testing.T) {
	tests := []struct {
		name  string
		page  int64
		limit int64
		want  int64
	}{
		{
			name:  "deve calcular offset para primeira página",
			page:  1,
			limit: 10,
			want:  0,
		},
		{
			name:  "deve calcular offset para segunda página",
			page:  2,
			limit: 10,
			want:  10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Skip(tt.page, tt.limit); got != tt.want {
				t.Errorf("Skip() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewCursor(t *testing.T) {
	items := []TestItem{
		{ID: 1, Name: "Item 1"},
		{ID: 2, Name: "Item 2"},
		{ID: 3, Name: "Item 3"},
	}

	next := func(item TestItem) string {
		return item.Name
	}

	tests := []struct {
		name  string
		items []TestItem
		limit int64
		want  *Cursor[TestItem]
	}{
		{
			name:  "deve criar cursor com próxima página",
			items: items,
			limit: 2,
			want: &Cursor[TestItem]{
				Items: items[:2],
				Next:  strPtr("Item 3"),
			},
		},
		{
			name:  "deve criar cursor sem próxima página",
			items: items[:2],
			limit: 2,
			want: &Cursor[TestItem]{
				Items: items[:2],
				Next:  nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewCursor(tt.items, tt.limit, next)
			if !compareCursor(got, tt.want) {
				t.Errorf("NewCursor() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Funções auxiliares para comparação
func comparePage[T any](a, b *Page[T]) bool {
	if len(a.Items) != len(b.Items) {
		return false
	}

	meta := a.Meta == b.Meta

	return meta
}

func compareCursor[T any](a, b *Cursor[T]) bool {
	if len(a.Items) != len(b.Items) {
		return false
	}

	if (a.Next == nil && b.Next != nil) || (a.Next != nil && b.Next == nil) {
		return false
	}

	if a.Next != nil && *a.Next != *b.Next {
		return false
	}

	return true
}

func strPtr(s string) *string {
	return &s
}
