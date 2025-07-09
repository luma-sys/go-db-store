package store

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/luma-sys/go-db-store/page"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

type mongoStore[T any] struct {
	coll *mongo.Collection
}

// NewMongoStore cria um novo mongoStore
func NewMongoStore[T any](coll *mongo.Collection) Store[T] {
	return &mongoStore[T]{
		coll: coll,
	}
}

func (s *mongoStore[T]) WithTransaction(ctx context.Context, fn Transaction) (any, error) {
	wc := writeconcern.Majority()
	txnOptions := options.Transaction().SetWriteConcern(wc)

	session, err := s.coll.Database().Client().StartSession()
	if err != nil {
		return nil, err
	}
	defer session.EndSession(ctx)

	result, err := session.WithTransaction(ctx, func(sessCtx context.Context) (any, error) {
		return fn(sessCtx)
	}, txnOptions)

	return result, err
}

// FindAll recupera documentos com paginação e filtros
func (s *mongoStore[T]) FindAll(ctx context.Context, f map[string]any, opts FindOptions) ([]T, error) {
	opts.Initialize()

	// Usando o filtro fornecido ou um filtro vazio se nenhum for fornecido
	filter := s.mapToBsonD(f)
	findOpts := options.Find()

	// Configurando a paginação
	if opts.Limit > 0 {
		skip := page.Skip(opts.Page, opts.Limit)
		findOpts.SetSkip(skip)
		findOpts.SetLimit(opts.Limit)
	}

	// Configurando a ordenação
	if opts.SortBy != "" {
		sortValue := 1
		if opts.OrderBy == "DESC" {
			sortValue = -1
		}
		findOpts.SetSort(bson.D{{Key: opts.SortBy, Value: sortValue}})
	}

	cursor, err := s.coll.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, fmt.Errorf("erro ao buscar documentos: %w", err)
	}
	defer cursor.Close(ctx)

	var results []T
	if err = cursor.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("erro ao decodificar documentos: %w", err)
	}

	return results, nil
}

// Count retorna o total de registros
func (s *mongoStore[T]) Count(ctx context.Context, f map[string]any) (*int64, error) {
	filter := s.mapToBsonD(f)

	total, err := s.coll.CountDocuments(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("erro ao quantificar documentos: %w", err)
	}

	return &total, nil
}

// FindById recupera um documento pelo ID
func (s *mongoStore[T]) FindById(ctx context.Context, id any) (*T, error) {
	var result T

	filter := bson.M{"_id": id}
	err := s.coll.FindOne(ctx, filter).Decode(&result)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("documento não encontrado com id %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("erro ao buscar documento: %w", err)
	}

	return &result, nil
}

// Save salva um documento
func (s *mongoStore[T]) Save(ctx context.Context, e *T) (*T, error) {
	now := time.Now()
	value := reflect.ValueOf(e).Elem()

	if created := value.FieldByName("CreatedAt"); created.IsValid() {
		created.Set(reflect.ValueOf(now))
	}
	if updated := value.FieldByName("UpdatedAt"); updated.IsValid() {
		updated.Set(reflect.ValueOf(now))
	}

	_, err := s.coll.InsertOne(ctx, e)
	if err != nil {
		return nil, fmt.Errorf("erro ao salvar documento: %w", err)
	}

	return e, nil
}

// SaveMany salva vários documentos
func (s *mongoStore[T]) SaveMany(ctx context.Context, e []T) (*InsertManyResult, error) {
	now := time.Now()

	docs := make([]any, len(e))
	for i, doc := range e {
		value := reflect.ValueOf(&doc).Elem()

		if created := value.FieldByName("CreatedAt"); created.IsValid() {
			created.Set(reflect.ValueOf(now))
		}
		if updated := value.FieldByName("UpdatedAt"); updated.IsValid() {
			updated.Set(reflect.ValueOf(now))
		}

		docs[i] = doc
	}

	result, err := s.coll.InsertMany(ctx, docs)
	if err != nil {
		return nil, fmt.Errorf("erro ao criar documentos: %w", err)
	}

	return &InsertManyResult{InsertedIDs: result.InsertedIDs}, nil
}

// SaveMany salva vários documentos
func (s *mongoStore[T]) SaveManyNotOrdered(ctx context.Context, e []T) (*InsertManyResult, error) {
	now := time.Now()

	docs := make([]any, len(e))
	for i, doc := range e {
		value := reflect.ValueOf(&doc).Elem()

		if created := value.FieldByName("CreatedAt"); created.IsValid() {
			created.Set(reflect.ValueOf(now))
		}
		if updated := value.FieldByName("UpdatedAt"); updated.IsValid() {
			updated.Set(reflect.ValueOf(now))
		}

		docs[i] = doc
	}

	result, err := s.coll.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if err != nil {
		return nil, fmt.Errorf("erro ao criar documentos: %w", err)
	}

	return &InsertManyResult{InsertedIDs: result.InsertedIDs}, nil
}

// Update atualiza um documento
func (s *mongoStore[T]) Update(ctx context.Context, e *T) (*T, error) {
	now := time.Now()
	value := reflect.ValueOf(e).Elem()
	id := value.FieldByName("ID").String()

	if updated := value.FieldByName("UpdatedAt"); updated.IsValid() {
		updated.Set(reflect.ValueOf(now))
	}

	filter := bson.M{"_id": id}
	update := bson.M{"$set": e}
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)

	var updated T
	err := s.coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&updated)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("documento não encontrado para atualização")
	}
	if err != nil {
		return nil, fmt.Errorf("erro ao atualizar documento: %w", err)
	}

	return &updated, nil
}

// UpdateMany atualiza múltiplos documentos baseado em um filtro genérico
func (s *mongoStore[T]) UpdateMany(ctx context.Context, f map[string]any, d map[string]any) (*UpdateResult, error) {
	if f == nil {
		return nil, fmt.Errorf("filtro não pode ser nulo")
	}

	filter := s.mapToBsonD(f)
	d["updatedAt"] = time.Now()
	payload := bson.D{{Key: "$set", Value: d}}

	result, err := s.coll.UpdateMany(ctx, filter, payload)
	if err != nil {
		return nil, fmt.Errorf("erro ao atualizar documentos: %w", err)
	}

	return &UpdateResult{
		MatchedCount:  result.MatchedCount,
		ModifiedCount: result.ModifiedCount,
		UpsertedCount: result.UpsertedCount,
		UpsertedID:    result.UpsertedID,
	}, nil
}

func (s *mongoStore[T]) Upsert(ctx context.Context, e *T, f []StoreUpsertFilter) (*UpdateResult, error) {
	now := time.Now()
	value := reflect.ValueOf(e).Elem()

	if updated := value.FieldByName("UpdatedAt"); updated.IsValid() {
		updated.Set(reflect.ValueOf(now))
	}

	var id string
	if fieldValue := value.FieldByName("ID"); fieldValue.IsValid() {
		id = fieldValue.String()
	}

	if len(f) == 0 {
		f = []StoreUpsertFilter{
			{
				UpsertFieldKey: "_id",
				UpsertBsonKey:  "ID",
			},
		}
	}

	filter, err := s.convertStoreUpsertFilterToBsonD(value, f)
	if err != nil {
		return nil, err
	}

	update := bson.M{
		"$set":         s.normalizeDocForUpsert(e),
		"$setOnInsert": bson.M{"_id": id, "createdAt": now},
	}

	result, err := s.coll.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true))
	if err != nil {
		return nil, fmt.Errorf("erro ao atualizar documento: %w", err)
	}

	return &UpdateResult{
		MatchedCount:  result.MatchedCount,
		ModifiedCount: result.ModifiedCount,
		UpsertedCount: result.UpsertedCount,
		UpsertedID:    result.UpsertedID,
	}, nil
}

func (s *mongoStore[T]) UpsertMany(ctx context.Context, e []T, f []StoreUpsertFilter) (*BulkWriteResult, error) {
	now := time.Now()
	operations := make([]mongo.WriteModel, len(e))

	for i, doc := range e {
		value := reflect.ValueOf(&doc).Elem()

		if updated := value.FieldByName("UpdatedAt"); updated.IsValid() {
			updated.Set(reflect.ValueOf(now))
		}

		fieldValue := value.FieldByName("ID")
		if !fieldValue.IsValid() {
			return nil, fmt.Errorf("invalid id from %d", i)
		}
		id := fieldValue.String()

		if len(f) == 0 {
			f = []StoreUpsertFilter{
				{
					UpsertFieldKey: "ID",
					UpsertBsonKey:  "_id",
				},
			}
		}

		filter, err := s.convertStoreUpsertFilterToBsonD(value, f)
		if err != nil {
			return nil, err
		}

		update := bson.M{
			"$set":         s.normalizeDocForUpsert(doc),
			"$setOnInsert": bson.M{"_id": id, "createdAt": now},
		}

		operations[i] = mongo.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update).
			SetUpsert(true)
	}

	result, err := s.coll.BulkWrite(ctx, operations)
	if err != nil {
		return nil, fmt.Errorf("erro ao atualizar documentos: %w", err)
	}

	return &BulkWriteResult{
		InsertedCount: result.InsertedCount,
		MatchedCount:  result.MatchedCount,
		ModifiedCount: result.ModifiedCount,
		DeletedCount:  result.DeletedCount,
		UpsertedCount: result.UpsertedCount,
		UpsertedIDs:   result.UpsertedIDs,
	}, nil
}

// Delete exclui um documento
func (s *mongoStore[T]) Delete(ctx context.Context, id any) error {
	result, err := s.coll.DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return fmt.Errorf("erro ao deletar documento: %w", err)
	}

	if result.DeletedCount == 0 {
		return fmt.Errorf("nenhum documento encontrado com id %s", id)
	}

	return nil
}

func (s *mongoStore[T]) DeleteMany(ctx context.Context, f map[string]any) (*DeleteResult, error) {
	if f == nil {
		return nil, fmt.Errorf("filtro não pode ser nulo")
	}

	filter := s.mapToBsonD(f)
	result, err := s.coll.DeleteMany(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("erro ao deletar documentos: %w", err)
	}

	return &DeleteResult{result.DeletedCount}, nil
}

// Has verifica se um documento existe
func (s *mongoStore[T]) Has(ctx context.Context, id any) bool {
	res, err := s.coll.Find(ctx, bson.M{"_id": id}, options.Find().SetLimit(1))
	if err != nil {
		return false
	}

	return res.RemainingBatchLength() == 1
}

// MapToBsonD converte um mapa genérico para bson.D
func (s *mongoStore[T]) mapToBsonD(m map[string]any) bson.D {
	bsonD := bson.D{}
	for key, value := range m {
		bsonD = append(bsonD, bson.E{Key: key, Value: value})
	}

	return bsonD
}

func (s *mongoStore[T]) normalizeDocForUpsert(doc any) bson.M {
	data, err := bson.Marshal(doc)
	if err != nil {
		return nil
	}

	var normalized bson.M
	if err = bson.Unmarshal(data, &normalized); err != nil {
		return nil
	}

	delete(normalized, "_id")
	delete(normalized, "createdAt")

	return normalized
}

func getFieldValue(key string, value reflect.Value) (any, error) {
	for k := range strings.SplitSeq(key, ".") {
		value = value.FieldByName(k)
		if !value.IsValid() {
			return nil, fmt.Errorf("invalid value")
		}
	}
	return value.Interface(), nil
}

func (s *mongoStore[T]) convertStoreUpsertFilterToBsonD(value reflect.Value, filters []StoreUpsertFilter) (bson.D, error) {
	var bsonD bson.D
	for _, filter := range filters {
		fieldValue, err := getFieldValue(filter.UpsertFieldKey, value)
		if err != nil {
			return nil, fmt.Errorf("invalid upsert field name from %s", filter.UpsertFieldKey)
		}

		bsonD = append(bsonD, bson.E{
			Key:   filter.UpsertBsonKey,
			Value: fieldValue,
		})
	}

	return bsonD, nil
}
