package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tryvium-travels/memongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type TestEntity struct {
	ID        string    `bson:"_id"`
	Name      string    `bson:"name"`
	Age       int       `bson:"age"`
	Active    bool      `bson:"active"`
	CreatedAt time.Time `bson:"createdAt"`
	UpdatedAt time.Time `bson:"updatedAt"`
}

func setupMongoTest(t *testing.T) (*mongo.Collection, func()) {
	mongoServer, err := memongo.Start("4.0.5")
	if err != nil {
		t.Fatalf("Erro ao iniciar MongoDB em memória: %v", err)
	}

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoServer.URI()))
	if err != nil {
		t.Fatalf("Erro ao conectar ao MongoDB: %v", err)
	}

	db := client.Database("test_db")
	collection := db.Collection("test_collection")

	// Função de cleanup
	cleanup := func() {
		collection.Drop(ctx)
		client.Disconnect(ctx)
	}

	return collection, cleanup
}

func TestMongoStore(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	t.Run("CRUD Operations", func(t *testing.T) {
		// Test Save
		t.Run("Save", func(t *testing.T) {
			entity := &TestEntity{
				ID:     "1",
				Name:   "Test Entity",
				Age:    25,
				Active: true,
			}

			saved, err := store.Save(ctx, entity)
			assert.NoError(t, err)
			assert.NotNil(t, saved)
			assert.NotZero(t, saved.CreatedAt)
			assert.NotZero(t, saved.UpdatedAt)
		})

		// Test FindById
		t.Run("FindById", func(t *testing.T) {
			found, err := store.FindById(ctx, "1")
			assert.NoError(t, err)
			assert.NotNil(t, found)
			assert.Equal(t, "Test Entity", found.Name)
		})

		// Test Update
		t.Run("Update", func(t *testing.T) {
			entity := &TestEntity{
				ID:   "1",
				Name: "Updated Entity",
				Age:  26,
			}

			updated, err := store.Update(ctx, entity)
			assert.NoError(t, err)
			assert.NotNil(t, updated)
			assert.Equal(t, "Updated Entity", updated.Name)
			assert.Equal(t, 26, updated.Age)
		})

		// Test Count
		t.Run("Count", func(t *testing.T) {
			count, err := store.Count(ctx, bson.M{})
			assert.NoError(t, err)
			assert.Equal(t, int64(1), *count)
		})

		// Test Has
		t.Run("Has", func(t *testing.T) {
			exists := store.Has(ctx, "1")
			assert.True(t, exists)

			exists = store.Has(ctx, "999")
			assert.False(t, exists)
		})

		// Test FindAll
		t.Run("FindAll", func(t *testing.T) {
			// Adiciona mais alguns registros para teste
			entities := []TestEntity{
				{ID: "2", Name: "Entity 2", Age: 30},
				{ID: "3", Name: "Entity 3", Age: 35},
			}

			for _, e := range entities {
				_, err := store.Save(ctx, &e)
				assert.NoError(t, err)
			}

			// Testa busca com filtro
			results, err := store.FindAll(ctx, map[string]any{"age": bson.M{"$gt": 30}}, FindOptions{})
			assert.NoError(t, err)
			assert.Equal(t, 1, len(results))

			// Testa paginação
			results, err = store.FindAll(ctx, nil, FindOptions{Page: 1, Limit: 2})
			assert.NoError(t, err)
			assert.Equal(t, 2, len(results))
		})

		// Test Delete
		t.Run("Delete", func(t *testing.T) {
			err := store.Delete(ctx, "1")
			assert.NoError(t, err)

			exists := store.Has(ctx, "1")
			assert.False(t, exists)
		})
	})

	t.Run("Batch Operations", func(t *testing.T) {
		// Test SaveMany
		t.Run("SaveMany", func(t *testing.T) {
			entities := []TestEntity{
				{ID: "4", Name: "Batch 1", Age: 40},
				{ID: "5", Name: "Batch 2", Age: 45},
			}

			result, err := store.SaveMany(ctx, entities)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(result.InsertedIDs))
		})

		// Test UpdateMany
		t.Run("UpdateMany", func(t *testing.T) {
			filter := map[string]any{"age": bson.M{"$gte": 40}}
			updates := map[string]any{"active": true}

			result, err := store.UpdateMany(ctx, filter, updates)
			assert.NoError(t, err)
			assert.Equal(t, int64(2), result.ModifiedCount)
		})

		// Test DeleteMany
		t.Run("DeleteMany", func(t *testing.T) {
			filter := map[string]any{"age": bson.M{"$gte": 40}}
			result, err := store.DeleteMany(ctx, filter)
			assert.NoError(t, err)
			assert.Equal(t, int64(2), result.DeletedCount)
		})
	})

	t.Run("Upsert Operations", func(t *testing.T) {
		// Test Upsert
		t.Run("Upsert", func(t *testing.T) {
			entity := &TestEntity{
				ID:     "6",
				Name:   "Upsert Test",
				Age:    50,
				Active: true,
			}

			result, err := store.Upsert(ctx, entity, nil)
			assert.NoError(t, err)
			assert.Equal(t, int64(1), result.UpsertedCount)

			// Teste de atualização via upsert
			entity.Age = 51
			result, err = store.Upsert(ctx, entity, nil)
			assert.NoError(t, err)
			assert.Equal(t, int64(1), result.ModifiedCount)
		})
	})
}
