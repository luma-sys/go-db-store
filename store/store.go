package store

import (
	"context"
	"database/sql"
)

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
	DeletedCount int64 `bson:"n"`
}

type FindOptions struct {
	Page    int64
	Limit   int64 // the 0 value of limit meens the will return all items
	OrderBy string
	SortBy  string
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
	WithTransaction(ctx context.Context, fn Transaction) (any, error)
	Has(ctx context.Context, id any) bool
	Count(ctx context.Context, q map[string]any) (*int64, error)

	FindById(ctx context.Context, id any) (*T, error)
	FindAll(ctx context.Context, q map[string]any, opts FindOptions) ([]T, error)

	Save(ctx context.Context, e *T) (*T, error)
	SaveMany(ctx context.Context, e []T) (*InsertManyResult, error)

	Update(ctx context.Context, e *T) (*T, error)
	UpdateMany(ctx context.Context, f map[string]any, d map[string]any) (*UpdateResult, error)

	Upsert(ctx context.Context, e *T, f []StoreUpsertFilter) (*UpdateResult, error)
	UpsertMany(ctx context.Context, e []T, f []StoreUpsertFilter) (*BulkWriteResult, error)

	Delete(ctx context.Context, id any) error
	DeleteMany(ctx context.Context, f map[string]any) (*DeleteResult, error)
}
