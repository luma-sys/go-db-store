package store

import (
	"context"
	"database/sql"
	"errors"
)

// ErrNotFound é embrulhado pelas buscas e updates por id quando o registro não existe; use errors.Is.
var ErrNotFound = errors.New("documento não encontrado")

type TransactionContext any

// Make sure mongo and sql implements our interface
var (
	_ TransactionContext = (*context.Context)(nil)
	_ TransactionContext = (*sql.Tx)(nil)
)

type Transaction func(ctx TransactionContext) (any, error)

type StoreUpsertFilter struct {
	UpsertFieldKey string
	UpsertBsonKey  string
}

type EntityFieldsToUpdate struct {
	Filter map[string]any `json:"filter"`
	Fields map[string]any `json:"fields"`
}

type BulkWriteResult struct {
	InsertedCount int64
	MatchedCount  int64
	ModifiedCount int64
	DeletedCount  int64
	UpsertedCount int64
	UpsertedIDs   map[int64]any
}

type InsertOneResult struct {
	InsertedID any
}

type InsertManyResult struct {
	InsertedIDs []any
}

type UpdateResult struct {
	MatchedCount  int64
	ModifiedCount int64
	UpsertedCount int64
	UpsertedID    any
}

type DeleteResult struct {
	DeletedCount int64
}

type FindOptions struct {
	Page    int64
	Limit   int64 // the 0 value of limit means the will return all items
	OrderBy string
	SortBy  string
	// Fields projeta a leitura: "campo" inclui, "-campo" exclui. Vazio devolve tudo.
	Fields []string
	// Sort ordena por vários campos, na ordem dada; quando preenchido, SortBy e OrderBy são ignorados.
	Sort []SortField
}

type SortField struct {
	Key  string
	Desc bool
}

func (o *FindOptions) Initialize() {
	if o.Page < 1 {
		o.Page = 1
	}
	if o.Limit < 0 {
		o.Limit = 10
	}
	if o.SortBy == "" {
		o.SortBy = "createdAt"
	}
	if o.OrderBy == "" {
		o.OrderBy = "ASC"
	}
}

type Store[T any] interface {
	EnsureIndexes(defs ...*IndexDefinition) error

	WithTransaction(ctx context.Context, fn Transaction) (any, error)
	Has(ctx context.Context, id any) bool
	Count(ctx context.Context, f map[string]any) (*int64, error)

	FindAll(ctx context.Context, f map[string]any, opts FindOptions) ([]T, error)
	FindById(ctx context.Context, id any) (*T, error)
	FindOne(ctx context.Context, f map[string]any) (*T, error)
	// FindOneWith é o FindOne com Fields e Sort de FindOptions; os demais campos são ignorados.
	FindOneWith(ctx context.Context, f map[string]any, opts FindOptions) (*T, error)

	Save(ctx context.Context, e *T) (*T, error)
	SaveMany(ctx context.Context, e []T) (*InsertManyResult, error)
	SaveManyNotOrdered(ctx context.Context, e []T) (*InsertManyResult, error)

	Update(ctx context.Context, e *T) (*T, error)
	// UpdateFields grava só os campos informados, sem ler e regravar a entidade inteira.
	UpdateFields(ctx context.Context, id any, fields map[string]any) (*T, error)
	UpdateMany(ctx context.Context, fd []EntityFieldsToUpdate) (*BulkWriteResult, error)

	Upsert(ctx context.Context, e *T, f []StoreUpsertFilter) (*UpdateResult, error)
	UpsertMany(ctx context.Context, e []T, f []StoreUpsertFilter) (*BulkWriteResult, error)

	Delete(ctx context.Context, id any) error
	DeleteOne(ctx context.Context, f map[string]any) error
	DeleteMany(ctx context.Context, f map[string]any) (*DeleteResult, error)
}
