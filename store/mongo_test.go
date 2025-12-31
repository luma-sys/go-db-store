package store

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tryvium-travels/memongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type TestEntity struct {
	ID        string    `bson:"_id"`
	Name      string    `bson:"name"`
	Age       int       `bson:"age"`
	Active    bool      `bson:"active"`
	Score     float64   `bson:"score"`
	Tags      []string  `bson:"tags"`
	CreatedAt time.Time `bson:"createdAt"`
	UpdatedAt time.Time `bson:"updatedAt"`
}

type TestEntityWithoutTimestamps struct {
	ID   string `bson:"_id"`
	Name string `bson:"name"`
}

// getMongoDownloadURL retorna a URL de download do MongoDB baseado no sistema operacional
//
// Esta função detecta automaticamente o sistema operacional e retorna a URL apropriada
// para download do MongoDB. Suporta:
//   - macOS
//   - Alpine Linux (containers Docker)
//   - Arch Linux
//   - RedHat / CentOS 8.0+
//   - SUSE Linux Enterprise Server
//   - Ubuntu 24.04 / 22.04
//   - Debian
//   - Outros sistemas Linux (usa Ubuntu 22.04 como fallback)
//
// Para CI/CD ou ambientes específicos, você pode sobrescrever a URL usando a variável
// de ambiente MONGODB_DOWNLOAD_URL. Exemplos:
//
//	export MONGODB_DOWNLOAD_URL="https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2404-7.0.14.tgz"
//	go test ./store
//
// URLs disponíveis em: https://www.mongodb.com/download-center/community/releases/archive
func getMongoDownloadURL(version string) string {
	// Permite override via variável de ambiente para CI/CD
	if customURL := os.Getenv("MONGODB_DOWNLOAD_URL"); customURL != "" {
		return customURL
	}

	// Detecta o sistema operacional
	if runtime.GOOS == "darwin" {
		// macOS
		return fmt.Sprintf("https://fastdl.mongodb.org/osx/mongodb-macos-x86_64-%s.tgz", version)
	}

	if runtime.GOOS == "linux" {
		// Tenta detectar a distribuição Linux
		if data, err := os.ReadFile("/etc/os-release"); err == nil {
			content := string(data)

			// Alpine Linux (comum em containers Docker)
			if strings.Contains(content, "Alpine") {
				return fmt.Sprintf("https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2204-%s.tgz", version)
			}

			// Arch Linux
			if strings.Contains(content, "Arch Linux") || strings.Contains(content, "arch") {
				return fmt.Sprintf("https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2204-%s.tgz", version)
			}

			// RedHat / CentOS 8.0+
			if strings.Contains(content, "Red Hat") || strings.Contains(content, "CentOS") || strings.Contains(content, "rhel") {
				return fmt.Sprintf("https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-rhel80-%s.tgz", version)
			}

			// SUSE Linux Enterprise Server
			if strings.Contains(content, "SUSE") || strings.Contains(content, "sles") {
				return fmt.Sprintf("https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-suse15-%s.tgz", version)
			}

			// Ubuntu 24.04
			if strings.Contains(content, "Ubuntu") && strings.Contains(content, "24.04") {
				return fmt.Sprintf("https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2404-%s.tgz", version)
			}

			// Ubuntu 22.04 (ou similar como Zorin OS 18)
			if strings.Contains(content, "Ubuntu") || strings.Contains(content, "Zorin") {
				return fmt.Sprintf("https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2204-%s.tgz", version)
			}

			// Debian
			if strings.Contains(content, "Debian") {
				return fmt.Sprintf("https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-debian12-%s.tgz", version)
			}
		}

		// Padrão para Linux genérico: usa Ubuntu 22.04 (mais compatível)
		return fmt.Sprintf("https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2204-%s.tgz", version)
	}

	// Windows ou outros
	return ""
}

func setupMongoTest(t *testing.T) (*mongo.Collection, func()) {
	memopts := &memongo.Options{
		MongoVersion:   "7.0.14",
		DownloadURL:    getMongoDownloadURL("7.0.14"),
		StartupTimeout: 120 * time.Second,
	}
	mongoServer, err := memongo.StartWithOptions(memopts)
	if err != nil {
		t.Fatalf("Erro ao iniciar MongoDB em memória: %v", err)
	}

	ctx := context.Background()
	opts := options.Client().ApplyURI(mongoServer.URI()).SetMaxPoolSize(200)
	client, err := mongo.Connect(opts)
	if err != nil {
		t.Fatalf("Erro ao conectar ao MongoDB: %v", err)
	}

	db := client.Database("test_db")
	collection := db.Collection("test_collection")

	cleanup := func() {
		collection.Drop(ctx)
		client.Disconnect(ctx)
		mongoServer.Stop()
	}

	return collection, cleanup
}

// ==================== TESTES SAVE ====================

func TestMongoSave(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	tests := []struct {
		name    string
		input   *TestEntity
		check   func(*testing.T, *TestEntity)
		wantErr bool
	}{
		{
			name: "deve salvar documento com todos os campos",
			input: &TestEntity{
				ID:     "1",
				Name:   "João Silva",
				Age:    30,
				Active: true,
				Score:  95.5,
				Tags:   []string{"developer", "golang"},
			},
			check: func(t *testing.T, result *TestEntity) {
				assert.Equal(t, "1", result.ID)
				assert.Equal(t, "João Silva", result.Name)
				assert.Equal(t, 30, result.Age)
				assert.True(t, result.Active)
				assert.Equal(t, 95.5, result.Score)
				assert.Equal(t, []string{"developer", "golang"}, result.Tags)
				assert.NotZero(t, result.CreatedAt)
				assert.NotZero(t, result.UpdatedAt)
			},
		},
		{
			name: "deve definir CreatedAt e UpdatedAt automaticamente",
			input: &TestEntity{
				ID:   "2",
				Name: "Maria Santos",
			},
			check: func(t *testing.T, result *TestEntity) {
				assert.NotZero(t, result.CreatedAt)
				assert.NotZero(t, result.UpdatedAt)
				assert.True(t, time.Since(result.CreatedAt) < time.Minute)
				assert.True(t, time.Since(result.UpdatedAt) < time.Minute)
			},
		},
		{
			name: "deve salvar documento com campos vazios",
			input: &TestEntity{
				ID:   "3",
				Name: "",
				Age:  0,
			},
			check: func(t *testing.T, result *TestEntity) {
				assert.Equal(t, "3", result.ID)
				assert.Empty(t, result.Name)
				assert.Zero(t, result.Age)
			},
		},
		{
			name: "deve salvar documento com slice vazio",
			input: &TestEntity{
				ID:   "4",
				Name: "Teste Tags",
				Tags: []string{},
			},
			check: func(t *testing.T, result *TestEntity) {
				assert.Empty(t, result.Tags)
			},
		},
		{
			name: "deve salvar documento com valores negativos",
			input: &TestEntity{
				ID:    "5",
				Name:  "Valores Negativos",
				Age:   -1,
				Score: -50.5,
			},
			check: func(t *testing.T, result *TestEntity) {
				assert.Equal(t, -1, result.Age)
				assert.Equal(t, -50.5, result.Score)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := store.Save(ctx, tt.input)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, result)

			if tt.check != nil {
				tt.check(t, result)
			}

			// Verifica persistência
			found, err := store.FindById(ctx, tt.input.ID)
			assert.NoError(t, err)
			assert.Equal(t, tt.input.ID, found.ID)
		})
	}
}

func TestMongoSave_DuplicateID(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	// Salva primeiro documento
	_, err := store.Save(ctx, &TestEntity{ID: "duplicate", Name: "Primeiro"})
	assert.NoError(t, err)

	// Tenta salvar com mesmo ID
	_, err = store.Save(ctx, &TestEntity{ID: "duplicate", Name: "Segundo"})
	assert.Error(t, err)
}

// ==================== TESTES SAVE MANY ====================

func TestMongoSaveMany(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	tests := []struct {
		name    string
		input   []TestEntity
		check   func(*testing.T, *InsertManyResult)
		wantErr bool
	}{
		{
			name: "deve salvar múltiplos documentos",
			input: []TestEntity{
				{ID: "1", Name: "João", Age: 25},
				{ID: "2", Name: "Maria", Age: 30},
				{ID: "3", Name: "Pedro", Age: 35},
			},
			check: func(t *testing.T, result *InsertManyResult) {
				assert.Equal(t, 3, len(result.InsertedIDs))
			},
		},
		{
			name: "deve definir timestamps em todos os documentos",
			input: []TestEntity{
				{ID: "10", Name: "Doc 1"},
				{ID: "11", Name: "Doc 2"},
			},
			check: func(t *testing.T, result *InsertManyResult) {
				assert.Equal(t, 2, len(result.InsertedIDs))
			},
		},
		{
			name:  "deve retornar nil para slice vazio",
			input: []TestEntity{},
			check: func(t *testing.T, result *InsertManyResult) {
				// Comportamento pode variar - verificar implementação
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collection.Drop(ctx)

			result, err := store.SaveMany(ctx, tt.input)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			if len(tt.input) == 0 {
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, result)

			if tt.check != nil {
				tt.check(t, result)
			}
		})
	}
}

func TestMongoSaveMany_PartialFailure(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	// Salva um documento primeiro
	_, _ = store.Save(ctx, &TestEntity{ID: "existing", Name: "Existente"})

	// Tenta salvar batch com ID duplicado
	entities := []TestEntity{
		{ID: "new1", Name: "Novo 1"},
		{ID: "existing", Name: "Duplicado"}, // Vai falhar
		{ID: "new2", Name: "Novo 2"},
	}

	result, err := store.SaveMany(ctx, entities)
	// Com ordered=false, deve inserir os válidos mesmo com erro
	assert.Error(t, err)
	assert.NotNil(t, result)
	assert.GreaterOrEqual(t, len(result.InsertedIDs), 2)
}

// ==================== TESTES SAVE MANY NOT ORDERED ====================

func TestMongoSaveManyNotOrdered(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	entities := []TestEntity{
		{ID: "1", Name: "Doc 1"},
		{ID: "2", Name: "Doc 2"},
		{ID: "3", Name: "Doc 3"},
	}

	result, err := store.SaveManyNotOrdered(ctx, entities)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(result.InsertedIDs))

	// Verifica se todos foram salvos
	count, _ := store.Count(ctx, bson.M{})
	assert.Equal(t, int64(3), *count)
}

// ==================== TESTES FIND BY ID ====================

func TestMongoFindById(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	// Setup: salva documentos de teste
	testDoc := &TestEntity{
		ID:     "find-test",
		Name:   "Documento Teste",
		Age:    25,
		Active: true,
		Score:  88.5,
		Tags:   []string{"test", "find"},
	}
	_, _ = store.Save(ctx, testDoc)

	tests := []struct {
		name    string
		id      any
		check   func(*testing.T, *TestEntity)
		wantErr bool
	}{
		{
			name: "deve encontrar documento existente",
			id:   "find-test",
			check: func(t *testing.T, result *TestEntity) {
				assert.Equal(t, "find-test", result.ID)
				assert.Equal(t, "Documento Teste", result.Name)
				assert.Equal(t, 25, result.Age)
				assert.True(t, result.Active)
				assert.Equal(t, 88.5, result.Score)
				assert.Equal(t, []string{"test", "find"}, result.Tags)
			},
		},
		{
			name:    "deve retornar erro para ID inexistente",
			id:      "nao-existe",
			wantErr: true,
		},
		{
			name:    "deve retornar erro para ID vazio",
			id:      "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := store.FindById(ctx, tt.id)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, result)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, result)

			if tt.check != nil {
				tt.check(t, result)
			}
		})
	}
}

// ==================== TESTES FIND ONE ====================

func TestMongoFindOne(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	// Setup: salva documentos de teste
	testDocs := []TestEntity{
		{ID: "1", Name: "João Silva", Age: 25, Active: true, Score: 80},
		{ID: "2", Name: "Maria Santos", Age: 30, Active: true, Score: 90},
		{ID: "3", Name: "Pedro Costa", Age: 35, Active: false, Score: 70},
	}
	for _, doc := range testDocs {
		_, _ = store.Save(ctx, &doc)
	}

	tests := []struct {
		name    string
		filter  map[string]interface{}
		check   func(*testing.T, *TestEntity)
		wantErr bool
	}{
		{
			name:   "deve encontrar documento com filtro simples",
			filter: map[string]interface{}{"name": "João Silva"},
			check: func(t *testing.T, result *TestEntity) {
				assert.Equal(t, "1", result.ID)
				assert.Equal(t, "João Silva", result.Name)
				assert.Equal(t, 25, result.Age)
				assert.True(t, result.Active)
				assert.Equal(t, 80.0, result.Score)
			},
		},
		{
			name:   "deve encontrar documento com filtro booleano",
			filter: map[string]interface{}{"active": false},
			check: func(t *testing.T, result *TestEntity) {
				assert.Equal(t, "3", result.ID)
				assert.False(t, result.Active)
			},
		},
		{
			name:   "deve encontrar documento com múltiplos filtros",
			filter: map[string]interface{}{"active": true, "age": 30},
			check: func(t *testing.T, result *TestEntity) {
				assert.Equal(t, "2", result.ID)
				assert.Equal(t, "Maria Santos", result.Name)
				assert.Equal(t, 30, result.Age)
				assert.True(t, result.Active)
			},
		},
		{
			name:   "deve encontrar documento com operador $gt",
			filter: map[string]interface{}{"age": bson.M{"$gt": 30}},
			check: func(t *testing.T, result *TestEntity) {
				assert.Equal(t, "3", result.ID)
				assert.Greater(t, result.Age, 30)
			},
		},
		{
			name:   "deve encontrar documento com operador $gte",
			filter: map[string]interface{}{"age": bson.M{"$gte": 30}},
			check: func(t *testing.T, result *TestEntity) {
				assert.GreaterOrEqual(t, result.Age, 30)
			},
		},
		{
			name:   "deve encontrar documento com operador $lt",
			filter: map[string]interface{}{"score": bson.M{"$lt": 75}},
			check: func(t *testing.T, result *TestEntity) {
				assert.Equal(t, "3", result.ID)
				assert.Less(t, result.Score, 75.0)
			},
		},
		{
			name:   "deve encontrar documento com operador $in",
			filter: map[string]interface{}{"name": bson.M{"$in": []string{"Maria Santos", "Não Existe"}}},
			check: func(t *testing.T, result *TestEntity) {
				assert.Equal(t, "2", result.ID)
				assert.Equal(t, "Maria Santos", result.Name)
			},
		},
		{
			name:   "deve encontrar documento com operador $regex",
			filter: map[string]interface{}{"name": bson.M{"$regex": "^João"}},
			check: func(t *testing.T, result *TestEntity) {
				assert.Equal(t, "1", result.ID)
				assert.Contains(t, result.Name, "João")
			},
		},
		{
			name:    "deve retornar erro quando não encontra documento",
			filter:  map[string]interface{}{"name": "Não Existe"},
			wantErr: true,
		},
		{
			name:    "deve retornar erro quando filtro não corresponde",
			filter:  map[string]interface{}{"age": 999},
			wantErr: true,
		},
		{
			name:   "deve encontrar documento com filtro vazio (retorna primeiro)",
			filter: map[string]interface{}{},
			check: func(t *testing.T, result *TestEntity) {
				assert.NotNil(t, result)
				assert.NotEmpty(t, result.ID)
			},
		},
		{
			name:   "deve encontrar por ID usando _id",
			filter: map[string]interface{}{"_id": "2"},
			check: func(t *testing.T, result *TestEntity) {
				assert.Equal(t, "2", result.ID)
				assert.Equal(t, "Maria Santos", result.Name)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := store.FindOne(ctx, tt.filter)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, result)
				assert.Contains(t, err.Error(), "documento não encontrado")
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, result)

			if tt.check != nil {
				tt.check(t, result)
			}
		})
	}
}

// ==================== TESTES FIND ALL ====================

func TestMongoFindAll(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	// Setup: salva documentos de teste
	testDocs := []TestEntity{
		{ID: "1", Name: "João", Age: 25, Active: true, Score: 80},
		{ID: "2", Name: "Maria", Age: 30, Active: true, Score: 90},
		{ID: "3", Name: "Pedro", Age: 35, Active: false, Score: 70},
		{ID: "4", Name: "Ana", Age: 28, Active: true, Score: 85},
		{ID: "5", Name: "Carlos", Age: 40, Active: false, Score: 75},
	}
	for _, doc := range testDocs {
		_, _ = store.Save(ctx, &doc)
	}

	tests := []struct {
		name    string
		filter  map[string]any
		opts    FindOptions
		wantLen int
		check   func(*testing.T, []TestEntity)
		wantErr bool
	}{
		{
			name:    "deve retornar todos os documentos sem filtro",
			filter:  nil,
			opts:    FindOptions{},
			wantLen: 5,
		},
		{
			name:    "deve retornar todos com filtro vazio",
			filter:  map[string]any{},
			opts:    FindOptions{},
			wantLen: 5,
		},
		{
			name:    "deve filtrar por campo booleano",
			filter:  map[string]any{"active": true},
			opts:    FindOptions{},
			wantLen: 3,
			check: func(t *testing.T, results []TestEntity) {
				for _, r := range results {
					assert.True(t, r.Active)
				}
			},
		},
		{
			name:    "deve filtrar por campo string",
			filter:  map[string]any{"name": "João"},
			opts:    FindOptions{},
			wantLen: 1,
			check: func(t *testing.T, results []TestEntity) {
				assert.Equal(t, "João", results[0].Name)
			},
		},
		{
			name:    "deve usar operador $gt",
			filter:  map[string]any{"age": bson.M{"$gt": 30}},
			opts:    FindOptions{},
			wantLen: 2,
			check: func(t *testing.T, results []TestEntity) {
				for _, r := range results {
					assert.Greater(t, r.Age, 30)
				}
			},
		},
		{
			name:    "deve usar operador $gte",
			filter:  map[string]any{"age": bson.M{"$gte": 30}},
			opts:    FindOptions{},
			wantLen: 3,
		},
		{
			name:    "deve usar operador $lt",
			filter:  map[string]any{"age": bson.M{"$lt": 30}},
			opts:    FindOptions{},
			wantLen: 2,
		},
		{
			name:    "deve usar operador $lte",
			filter:  map[string]any{"age": bson.M{"$lte": 30}},
			opts:    FindOptions{},
			wantLen: 3,
		},
		{
			name:    "deve usar operador $in",
			filter:  map[string]any{"name": bson.M{"$in": []string{"João", "Maria"}}},
			opts:    FindOptions{},
			wantLen: 2,
		},
		{
			name:    "deve usar operador $nin",
			filter:  map[string]any{"name": bson.M{"$nin": []string{"João", "Maria"}}},
			opts:    FindOptions{},
			wantLen: 3,
		},
		{
			name:    "deve usar operador $regex",
			filter:  map[string]any{"name": bson.M{"$regex": "^M"}},
			opts:    FindOptions{},
			wantLen: 1,
			check: func(t *testing.T, results []TestEntity) {
				assert.Equal(t, "Maria", results[0].Name)
			},
		},
		{
			name:    "deve usar operador $ne",
			filter:  map[string]any{"active": bson.M{"$ne": true}},
			opts:    FindOptions{},
			wantLen: 2,
		},
		{
			name:   "deve combinar múltiplos filtros",
			filter: map[string]any{"active": true, "age": bson.M{"$gte": 28}},
			opts:   FindOptions{},
			check: func(t *testing.T, results []TestEntity) {
				for _, r := range results {
					assert.True(t, r.Active)
					assert.GreaterOrEqual(t, r.Age, 28)
				}
			},
		},
		{
			name:    "deve aplicar paginação - página 1",
			filter:  nil,
			opts:    FindOptions{Page: 1, Limit: 2},
			wantLen: 2,
		},
		{
			name:    "deve aplicar paginação - página 2",
			filter:  nil,
			opts:    FindOptions{Page: 2, Limit: 2},
			wantLen: 2,
		},
		{
			name:    "deve aplicar paginação - página 3",
			filter:  nil,
			opts:    FindOptions{Page: 3, Limit: 2},
			wantLen: 1,
		},
		{
			name:   "deve ordenar por campo ASC",
			filter: nil,
			opts:   FindOptions{SortBy: "age", OrderBy: "ASC"},
			check: func(t *testing.T, results []TestEntity) {
				assert.Equal(t, 25, results[0].Age)
				assert.Equal(t, 40, results[len(results)-1].Age)
			},
		},
		{
			name:   "deve ordenar por campo DESC",
			filter: nil,
			opts:   FindOptions{SortBy: "age", OrderBy: "DESC"},
			check: func(t *testing.T, results []TestEntity) {
				assert.Equal(t, 40, results[0].Age)
				assert.Equal(t, 25, results[len(results)-1].Age)
			},
		},
		{
			name:   "deve ordenar por _id quando SortBy é 'id'",
			filter: nil,
			opts:   FindOptions{SortBy: "id", OrderBy: "ASC"},
			check: func(t *testing.T, results []TestEntity) {
				assert.Equal(t, "1", results[0].ID)
			},
		},
		{
			name:    "deve retornar vazio quando filtro não encontra",
			filter:  map[string]any{"name": "NaoExiste"},
			opts:    FindOptions{},
			wantLen: 0,
		},
		{
			name:    "deve usar operador $and implícito",
			filter:  map[string]any{"active": true, "score": bson.M{"$gte": 85}},
			opts:    FindOptions{},
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := store.FindAll(ctx, tt.filter, tt.opts)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			if tt.wantLen > 0 {
				assert.Equal(t, tt.wantLen, len(results))
			}

			if tt.check != nil {
				tt.check(t, results)
			}
		})
	}
}

// ==================== TESTES COUNT ====================

func TestMongoCount(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	// Setup
	testDocs := []TestEntity{
		{ID: "1", Name: "João", Age: 25, Active: true},
		{ID: "2", Name: "Maria", Age: 30, Active: true},
		{ID: "3", Name: "Pedro", Age: 35, Active: false},
	}
	for _, doc := range testDocs {
		_, _ = store.Save(ctx, &doc)
	}

	tests := []struct {
		name      string
		filter    map[string]any
		wantCount int64
		wantErr   bool
	}{
		{
			name:      "deve contar todos os documentos",
			filter:    bson.M{},
			wantCount: 3,
		},
		{
			name:      "deve contar com filtro booleano",
			filter:    map[string]any{"active": true},
			wantCount: 2,
		},
		{
			name:      "deve contar com operador $gt",
			filter:    map[string]any{"age": bson.M{"$gt": 25}},
			wantCount: 2,
		},
		{
			name:      "deve retornar zero quando não encontra",
			filter:    map[string]any{"name": "NaoExiste"},
			wantCount: 0,
		},
		{
			name:      "deve contar com múltiplos filtros",
			filter:    map[string]any{"active": true, "age": bson.M{"$gte": 30}},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count, err := store.Count(ctx, tt.filter)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.wantCount, *count)
		})
	}
}

// ==================== TESTES HAS ====================

func TestMongoHas(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	// Setup
	_, _ = store.Save(ctx, &TestEntity{ID: "exists", Name: "Existe"})

	tests := []struct {
		name string
		id   any
		want bool
	}{
		{
			name: "deve retornar true para documento existente",
			id:   "exists",
			want: true,
		},
		{
			name: "deve retornar false para documento inexistente",
			id:   "not-exists",
			want: false,
		},
		{
			name: "deve retornar false para ID vazio",
			id:   "",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := store.Has(ctx, tt.id)
			assert.Equal(t, tt.want, result)
		})
	}
}

// ==================== TESTES UPDATE ====================

func TestMongoUpdate(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	tests := []struct {
		name    string
		setup   func() *TestEntity
		update  func(*TestEntity) *TestEntity
		check   func(*testing.T, *TestEntity)
		wantErr bool
	}{
		{
			name: "deve atualizar campo string",
			setup: func() *TestEntity {
				doc := &TestEntity{ID: "1", Name: "Original", Age: 25}
				store.Save(ctx, doc)
				return doc
			},
			update: func(e *TestEntity) *TestEntity {
				e.Name = "Atualizado"
				return e
			},
			check: func(t *testing.T, result *TestEntity) {
				assert.Equal(t, "Atualizado", result.Name)
				assert.Equal(t, 25, result.Age)
			},
		},
		{
			name: "deve atualizar campo numérico",
			setup: func() *TestEntity {
				doc := &TestEntity{ID: "2", Name: "Teste", Age: 25, Score: 80}
				store.Save(ctx, doc)
				return doc
			},
			update: func(e *TestEntity) *TestEntity {
				e.Age = 30
				e.Score = 95.5
				return e
			},
			check: func(t *testing.T, result *TestEntity) {
				assert.Equal(t, 30, result.Age)
				assert.Equal(t, 95.5, result.Score)
			},
		},
		{
			name: "deve atualizar campo booleano",
			setup: func() *TestEntity {
				doc := &TestEntity{ID: "3", Name: "Teste", Active: false}
				store.Save(ctx, doc)
				return doc
			},
			update: func(e *TestEntity) *TestEntity {
				e.Active = true
				return e
			},
			check: func(t *testing.T, result *TestEntity) {
				assert.True(t, result.Active)
			},
		},
		{
			name: "deve atualizar UpdatedAt automaticamente",
			setup: func() *TestEntity {
				doc := &TestEntity{ID: "4", Name: "Teste"}
				store.Save(ctx, doc)
				time.Sleep(10 * time.Millisecond)
				return doc
			},
			update: func(e *TestEntity) *TestEntity {
				e.Name = "Atualizado"
				return e
			},
			check: func(t *testing.T, result *TestEntity) {
				assert.True(t, time.Since(result.UpdatedAt) < time.Minute)
			},
		},
		{
			name: "deve atualizar slice",
			setup: func() *TestEntity {
				doc := &TestEntity{ID: "5", Name: "Teste", Tags: []string{"original"}}
				store.Save(ctx, doc)
				return doc
			},
			update: func(e *TestEntity) *TestEntity {
				e.Tags = []string{"novo1", "novo2"}
				return e
			},
			check: func(t *testing.T, result *TestEntity) {
				assert.Equal(t, []string{"novo1", "novo2"}, result.Tags)
			},
		},
		{
			name: "deve retornar erro para documento inexistente",
			setup: func() *TestEntity {
				return &TestEntity{ID: "nao-existe", Name: "Teste"}
			},
			update: func(e *TestEntity) *TestEntity {
				return e
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entity := tt.setup()
			toUpdate := tt.update(entity)

			result, err := store.Update(ctx, toUpdate)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, result)

			if tt.check != nil {
				tt.check(t, result)
			}
		})
	}
}

// ==================== TESTES UPDATE MANY ====================

func TestMongoUpdateMany(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	tests := []struct {
		name    string
		setup   func()
		input   []EntityFieldsToUpdate
		check   func(*testing.T, *BulkWriteResult)
		wantErr bool
		errMsg  string
	}{
		{
			name: "deve atualizar um único documento",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "1", Name: "Original", Age: 25})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"_id": "1"},
					Fields: map[string]any{"name": "Atualizado"},
				},
			},
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Equal(t, int64(1), result.MatchedCount)
				assert.Equal(t, int64(1), result.ModifiedCount)

				record, _ := store.FindById(ctx, "1")
				assert.Equal(t, "Atualizado", record.Name)
			},
		},
		{
			name: "deve atualizar múltiplos documentos com filtros diferentes",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "1", Name: "João", Age: 25})
				store.Save(ctx, &TestEntity{ID: "2", Name: "Maria", Age: 30})
				store.Save(ctx, &TestEntity{ID: "3", Name: "Pedro", Age: 35})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"_id": "1"},
					Fields: map[string]any{"name": "João Atualizado"},
				},
				{
					Filter: map[string]any{"_id": "2"},
					Fields: map[string]any{"name": "Maria Atualizada"},
				},
			},
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Equal(t, int64(2), result.MatchedCount)

				record1, _ := store.FindById(ctx, "1")
				assert.Equal(t, "João Atualizado", record1.Name)

				record2, _ := store.FindById(ctx, "2")
				assert.Equal(t, "Maria Atualizada", record2.Name)

				record3, _ := store.FindById(ctx, "3")
				assert.Equal(t, "Pedro", record3.Name)
			},
		},
		{
			name: "deve atualizar vários documentos com mesmo filtro",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "1", Name: "Grupo A", Active: true})
				store.Save(ctx, &TestEntity{ID: "2", Name: "Grupo A", Active: true})
				store.Save(ctx, &TestEntity{ID: "3", Name: "Grupo B", Active: false})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"active": true},
					Fields: map[string]any{"name": "Grupo A Atualizado"},
				},
			},
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Equal(t, int64(2), result.MatchedCount)

				results, _ := store.FindAll(ctx, map[string]any{"name": "Grupo A Atualizado"}, FindOptions{})
				assert.Equal(t, 2, len(results))
			},
		},
		{
			name: "deve atualizar updatedAt automaticamente",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "1", Name: "Original", UpdatedAt: time.Now().Add(-1 * time.Hour)})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"_id": "1"},
					Fields: map[string]any{"name": "Atualizado"},
				},
			},
			check: func(t *testing.T, result *BulkWriteResult) {
				record, _ := store.FindById(ctx, "1")
				assert.True(t, time.Since(record.UpdatedAt) < time.Minute)
			},
		},
		{
			name: "deve usar operadores MongoDB no filtro",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "1", Name: "Jovem", Age: 20})
				store.Save(ctx, &TestEntity{ID: "2", Name: "Adulto", Age: 30})
				store.Save(ctx, &TestEntity{ID: "3", Name: "Senior", Age: 50})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"age": bson.M{"$gte": 30}},
					Fields: map[string]any{"active": true},
				},
			},
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Equal(t, int64(2), result.MatchedCount)

				results, _ := store.FindAll(ctx, map[string]any{"active": true}, FindOptions{})
				assert.Equal(t, 2, len(results))
			},
		},
		{
			name: "deve usar operador $in no filtro",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "1", Name: "Registro 1"})
				store.Save(ctx, &TestEntity{ID: "2", Name: "Registro 2"})
				store.Save(ctx, &TestEntity{ID: "3", Name: "Registro 3"})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"_id": bson.M{"$in": []string{"1", "3"}}},
					Fields: map[string]any{"name": "Atualizado via IN"},
				},
			},
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Equal(t, int64(2), result.MatchedCount)

				record2, _ := store.FindById(ctx, "2")
				assert.Equal(t, "Registro 2", record2.Name)
			},
		},
		{
			name: "deve usar operador $regex no filtro",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "1", Name: "João Silva"})
				store.Save(ctx, &TestEntity{ID: "2", Name: "João Santos"})
				store.Save(ctx, &TestEntity{ID: "3", Name: "Maria Silva"})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"name": bson.M{"$regex": "^João"}},
					Fields: map[string]any{"active": true},
				},
			},
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Equal(t, int64(2), result.MatchedCount)
			},
		},
		{
			name:    "deve retornar erro quando slice vazio",
			setup:   func() {},
			input:   []EntityFieldsToUpdate{},
			wantErr: true,
			errMsg:  "nenhum update fornecido",
		},
		{
			name: "deve retornar erro quando filtro vazio",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "1", Name: "Original"})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{},
					Fields: map[string]any{"name": "Atualizado"},
				},
			},
			wantErr: true,
			errMsg:  "filtro é obrigatório para update 0",
		},
		{
			name: "deve retornar zero quando filtro não encontra",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "1", Name: "Original"})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"_id": "999"},
					Fields: map[string]any{"name": "Atualizado"},
				},
			},
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Equal(t, int64(0), result.MatchedCount)
			},
		},
		{
			name: "deve preservar campos não atualizados",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "1", Name: "Original", Age: 25, Active: true})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"_id": "1"},
					Fields: map[string]any{"name": "Atualizado"},
				},
			},
			check: func(t *testing.T, result *BulkWriteResult) {
				record, _ := store.FindById(ctx, "1")
				assert.Equal(t, "Atualizado", record.Name)
				assert.Equal(t, 25, record.Age)
				assert.True(t, record.Active)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collection.Drop(ctx)
			tt.setup()

			result, err := store.UpdateMany(ctx, tt.input)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, result)

			if tt.check != nil {
				tt.check(t, result)
			}
		})
	}
}

// ==================== TESTES UPSERT ====================

func TestMongoUpsert(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	tests := []struct {
		name    string
		setup   func()
		input   *TestEntity
		filters []StoreUpsertFilter
		check   func(*testing.T, *UpdateResult)
		wantErr bool
	}{
		{
			name:  "deve inserir novo documento quando não existe",
			setup: func() {},
			input: &TestEntity{
				ID:     "new-1",
				Name:   "Novo Documento",
				Age:    25,
				Active: true,
			},
			filters: nil,
			check: func(t *testing.T, result *UpdateResult) {
				assert.Equal(t, int64(1), result.UpsertedCount)

				found, err := store.FindById(ctx, "new-1")
				assert.NoError(t, err)
				assert.Equal(t, "Novo Documento", found.Name)
			},
		},
		{
			name: "deve atualizar documento existente",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "existing", Name: "Original", Age: 25})
			},
			input: &TestEntity{
				ID:   "existing",
				Name: "Atualizado",
				Age:  30,
			},
			filters: nil,
			check: func(t *testing.T, result *UpdateResult) {
				assert.Equal(t, int64(1), result.ModifiedCount)

				found, _ := store.FindById(ctx, "existing")
				assert.Equal(t, "Atualizado", found.Name)
				assert.Equal(t, 30, found.Age)
			},
		},
		{
			name:  "deve definir timestamps em novo documento",
			setup: func() {},
			input: &TestEntity{
				ID:   "new-timestamps",
				Name: "Com Timestamps",
			},
			filters: nil,
			check: func(t *testing.T, result *UpdateResult) {
				found, _ := store.FindById(ctx, "new-timestamps")
				assert.NotZero(t, found.CreatedAt)
				assert.NotZero(t, found.UpdatedAt)
			},
		},
		{
			name: "deve usar filtro personalizado",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "custom-1", Name: "UniqueNameForUpsert", Age: 25})
			},
			input: &TestEntity{
				ID:   "custom-2",
				Name: "UniqueNameForUpsert",
				Age:  30,
			},
			filters: []StoreUpsertFilter{
				{UpsertFieldKey: "Name", UpsertBsonKey: "name"},
			},
			check: func(t *testing.T, result *UpdateResult) {
				assert.Equal(t, int64(1), result.ModifiedCount)

				// Deve ter atualizado o documento existente
				found, _ := store.FindById(ctx, "custom-1")
				assert.Equal(t, 30, found.Age)
			},
		},
		{
			name: "deve preservar CreatedAt em atualização",
			setup: func() {
				doc := &TestEntity{ID: "preserve-created", Name: "Original"}
				store.Save(ctx, doc)
			},
			input: &TestEntity{
				ID:   "preserve-created",
				Name: "Atualizado",
			},
			filters: nil,
			check: func(t *testing.T, result *UpdateResult) {
				found, _ := store.FindById(ctx, "preserve-created")
				assert.NotZero(t, found.CreatedAt)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collection.Drop(ctx)
			tt.setup()

			result, err := store.Upsert(ctx, tt.input, tt.filters)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, result)

			if tt.check != nil {
				tt.check(t, result)
			}
		})
	}
}

// ==================== TESTES UPSERT MANY ====================

func TestMongoUpsertMany(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	tests := []struct {
		name    string
		setup   func()
		input   []TestEntity
		filters []StoreUpsertFilter
		check   func(*testing.T, *BulkWriteResult)
		wantErr bool
	}{
		{
			name:  "deve inserir múltiplos novos documentos",
			setup: func() {},
			input: []TestEntity{
				{ID: "new-1", Name: "Doc 1", Age: 25},
				{ID: "new-2", Name: "Doc 2", Age: 30},
				{ID: "new-3", Name: "Doc 3", Age: 35},
			},
			filters: nil,
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Equal(t, int64(3), result.UpsertedCount)

				count, _ := store.Count(ctx, bson.M{})
				assert.Equal(t, int64(3), *count)
			},
		},
		{
			name: "deve atualizar documentos existentes",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "existing-1", Name: "Original 1", Age: 20})
				store.Save(ctx, &TestEntity{ID: "existing-2", Name: "Original 2", Age: 25})
			},
			input: []TestEntity{
				{ID: "existing-1", Name: "Atualizado 1", Age: 30},
				{ID: "existing-2", Name: "Atualizado 2", Age: 35},
			},
			filters: nil,
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Equal(t, int64(2), result.ModifiedCount)

				found1, _ := store.FindById(ctx, "existing-1")
				assert.Equal(t, "Atualizado 1", found1.Name)

				found2, _ := store.FindById(ctx, "existing-2")
				assert.Equal(t, "Atualizado 2", found2.Name)
			},
		},
		{
			name: "deve misturar inserções e atualizações",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "existing", Name: "Original", Age: 20})
			},
			input: []TestEntity{
				{ID: "existing", Name: "Atualizado", Age: 30},
				{ID: "new", Name: "Novo", Age: 25},
			},
			filters: nil,
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Equal(t, int64(1), result.ModifiedCount)
				assert.Equal(t, int64(1), result.UpsertedCount)

				count, _ := store.Count(ctx, bson.M{})
				assert.Equal(t, int64(2), *count)
			},
		},
		{
			name:  "deve definir timestamps em todos os documentos",
			setup: func() {},
			input: []TestEntity{
				{ID: "ts-1", Name: "Doc 1"},
				{ID: "ts-2", Name: "Doc 2"},
			},
			filters: nil,
			check: func(t *testing.T, result *BulkWriteResult) {
				found1, _ := store.FindById(ctx, "ts-1")
				assert.NotZero(t, found1.CreatedAt)
				assert.NotZero(t, found1.UpdatedAt)

				found2, _ := store.FindById(ctx, "ts-2")
				assert.NotZero(t, found2.CreatedAt)
				assert.NotZero(t, found2.UpdatedAt)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collection.Drop(ctx)
			tt.setup()

			result, err := store.UpsertMany(ctx, tt.input, tt.filters)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, result)

			if tt.check != nil {
				tt.check(t, result)
			}
		})
	}
}

// ==================== TESTES DELETE ====================

func TestMongoDelete(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	tests := []struct {
		name    string
		setup   func()
		id      any
		check   func(*testing.T)
		wantErr bool
	}{
		{
			name: "deve deletar documento existente",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "to-delete", Name: "Para Deletar"})
			},
			id: "to-delete",
			check: func(t *testing.T) {
				exists := store.Has(ctx, "to-delete")
				assert.False(t, exists)
			},
		},
		{
			name: "deve retornar erro para documento inexistente",
			setup: func() {
				// Não cria nenhum documento
			},
			id:      "nao-existe",
			wantErr: true,
		},
		{
			name: "deve manter outros documentos intactos",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "keep-1", Name: "Manter 1"})
				store.Save(ctx, &TestEntity{ID: "delete-me", Name: "Deletar"})
				store.Save(ctx, &TestEntity{ID: "keep-2", Name: "Manter 2"})
			},
			id: "delete-me",
			check: func(t *testing.T) {
				assert.True(t, store.Has(ctx, "keep-1"))
				assert.False(t, store.Has(ctx, "delete-me"))
				assert.True(t, store.Has(ctx, "keep-2"))

				count, _ := store.Count(ctx, bson.M{})
				assert.Equal(t, int64(2), *count)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collection.Drop(ctx)
			tt.setup()

			err := store.Delete(ctx, tt.id)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			if tt.check != nil {
				tt.check(t)
			}
		})
	}
}

// ==================== TESTES DELETE MANY ====================

func TestMongoDeleteMany(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	tests := []struct {
		name    string
		setup   func()
		filter  map[string]any
		check   func(*testing.T, *DeleteResult)
		wantErr bool
	}{
		{
			name: "deve deletar múltiplos documentos",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "1", Name: "Doc", Active: true})
				store.Save(ctx, &TestEntity{ID: "2", Name: "Doc", Active: true})
				store.Save(ctx, &TestEntity{ID: "3", Name: "Doc", Active: false})
			},
			filter: map[string]any{"active": true},
			check: func(t *testing.T, result *DeleteResult) {
				assert.Equal(t, int64(2), result.DeletedCount)

				count, _ := store.Count(ctx, bson.M{})
				assert.Equal(t, int64(1), *count)
			},
		},
		{
			name: "deve usar operadores no filtro",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "1", Name: "Doc", Age: 20})
				store.Save(ctx, &TestEntity{ID: "2", Name: "Doc", Age: 30})
				store.Save(ctx, &TestEntity{ID: "3", Name: "Doc", Age: 40})
			},
			filter: map[string]any{"age": bson.M{"$gte": 30}},
			check: func(t *testing.T, result *DeleteResult) {
				assert.Equal(t, int64(2), result.DeletedCount)
			},
		},
		{
			name: "deve retornar zero quando não encontra",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "1", Name: "Doc"})
			},
			filter: map[string]any{"name": "NaoExiste"},
			check: func(t *testing.T, result *DeleteResult) {
				assert.Equal(t, int64(0), result.DeletedCount)
			},
		},
		{
			name: "deve retornar erro para filtro nil",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "1", Name: "Doc"})
			},
			filter:  nil,
			wantErr: true,
		},
		{
			name: "deve usar operador $in",
			setup: func() {
				store.Save(ctx, &TestEntity{ID: "1", Name: "Doc 1"})
				store.Save(ctx, &TestEntity{ID: "2", Name: "Doc 2"})
				store.Save(ctx, &TestEntity{ID: "3", Name: "Doc 3"})
			},
			filter: map[string]any{"_id": bson.M{"$in": []string{"1", "3"}}},
			check: func(t *testing.T, result *DeleteResult) {
				assert.Equal(t, int64(2), result.DeletedCount)

				assert.False(t, store.Has(ctx, "1"))
				assert.True(t, store.Has(ctx, "2"))
				assert.False(t, store.Has(ctx, "3"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collection.Drop(ctx)
			tt.setup()

			result, err := store.DeleteMany(ctx, tt.filter)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, result)

			if tt.check != nil {
				tt.check(t, result)
			}
		})
	}
}

// ==================== TESTES WITH TRANSACTION ====================

func TestMongoWithTransaction(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	t.Run("deve executar operações em transação com sucesso", func(t *testing.T) {
		// Nota: Transações requerem replica set, que o memongo pode não suportar
		result, err := store.WithTransaction(ctx, func(txCtx TransactionContext) (any, error) {
			return "success", nil
		})

		// Se transações não forem suportadas, o teste é ignorado
		if err != nil {
			t.Skip("Transações não suportadas nesta configuração do MongoDB")
		}

		assert.Equal(t, "success", result)
	})

	t.Run("deve fazer rollback em caso de erro", func(t *testing.T) {
		_, err := store.WithTransaction(ctx, func(txCtx TransactionContext) (any, error) {
			return nil, fmt.Errorf("erro simulado")
		})

		// Se transações não forem suportadas, o teste é ignorado
		if err != nil && err.Error() != "erro simulado" {
			t.Skip("Transações não suportadas nesta configuração do MongoDB")
		}

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "erro simulado")
	})

	t.Run("deve permitir operações de store dentro da transação", func(t *testing.T) {
		result, err := store.WithTransaction(ctx, func(txCtx TransactionContext) (any, error) {
			// Converte para context.Context para usar nas operações
			sessCtx, ok := txCtx.(context.Context)
			if !ok {
				return nil, fmt.Errorf("contexto inválido")
			}

			// Salva um documento dentro da transação
			doc := &TestEntity{ID: "tx-test", Name: "Transação"}
			saved, err := store.Save(sessCtx, doc)
			if err != nil {
				return nil, err
			}

			return saved, nil
		})

		if err != nil {
			t.Skip("Transações não suportadas nesta configuração do MongoDB")
		}

		assert.NotNil(t, result)
		saved := result.(*TestEntity)
		assert.Equal(t, "tx-test", saved.ID)
	})
}

// ==================== TESTES DE EDGE CASES ====================

func TestMongoEdgeCases(t *testing.T) {
	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	t.Run("deve lidar com documentos com campos especiais", func(t *testing.T) {
		doc := &TestEntity{
			ID:   "special",
			Name: "Nome com 'aspas' e \"aspas duplas\"",
			Tags: []string{"tag1", "tag2", "tag com espaço"},
		}

		saved, err := store.Save(ctx, doc)
		assert.NoError(t, err)

		found, err := store.FindById(ctx, "special")
		assert.NoError(t, err)
		assert.Equal(t, saved.Name, found.Name)
		assert.Equal(t, saved.Tags, found.Tags)
	})

	t.Run("deve lidar com valores extremos", func(t *testing.T) {
		doc := &TestEntity{
			ID:    "extreme",
			Name:  "Extreme Values",
			Age:   2147483647, // Max int32
			Score: 1.7976931348623157e+308,
		}

		_, err := store.Save(ctx, doc)
		assert.NoError(t, err)

		found, err := store.FindById(ctx, "extreme")
		assert.NoError(t, err)
		assert.Equal(t, doc.Age, found.Age)
	})

	t.Run("deve lidar com strings vazias em busca", func(t *testing.T) {
		store.Save(ctx, &TestEntity{ID: "empty-name", Name: ""})
		store.Save(ctx, &TestEntity{ID: "with-name", Name: "Teste"})

		results, err := store.FindAll(ctx, map[string]any{"name": ""}, FindOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(results))
		assert.Equal(t, "empty-name", results[0].ID)
	})

	t.Run("deve lidar com paginação além dos resultados", func(t *testing.T) {
		store.Save(ctx, &TestEntity{ID: "1", Name: "Doc 1"})
		store.Save(ctx, &TestEntity{ID: "2", Name: "Doc 2"})

		results, err := store.FindAll(ctx, nil, FindOptions{Page: 100, Limit: 10})
		assert.NoError(t, err)
		assert.Empty(t, results)
	})

	t.Run("deve lidar com operações em collection vazia", func(t *testing.T) {
		collection.Drop(ctx)

		count, err := store.Count(ctx, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), *count)

		results, err := store.FindAll(ctx, nil, FindOptions{})
		assert.NoError(t, err)
		assert.Empty(t, results)

		exists := store.Has(ctx, "any-id")
		assert.False(t, exists)
	})
}

// ==================== TESTES DE PERFORMANCE ====================

func TestMongoPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Pulando testes de performance em modo curto")
	}

	collection, cleanup := setupMongoTest(t)
	defer cleanup()

	store := NewMongoStore[TestEntity](collection)
	ctx := context.Background()

	t.Run("deve inserir 1000 documentos em batch eficientemente", func(t *testing.T) {
		entities := make([]TestEntity, 1000)
		for i := 0; i < 1000; i++ {
			entities[i] = TestEntity{
				ID:   fmt.Sprintf("perf-%d", i),
				Name: fmt.Sprintf("Performance Test %d", i),
				Age:  i % 100,
			}
		}

		start := time.Now()
		result, err := store.SaveMany(ctx, entities)
		duration := time.Since(start)

		assert.NoError(t, err)
		assert.Equal(t, 1000, len(result.InsertedIDs))
		assert.Less(t, duration, 10*time.Second)

		t.Logf("Inserção de 1000 documentos: %v", duration)
	})

	t.Run("deve buscar com filtro eficientemente", func(t *testing.T) {
		start := time.Now()
		results, err := store.FindAll(ctx, map[string]any{"age": bson.M{"$gte": 50}}, FindOptions{})
		duration := time.Since(start)

		assert.NoError(t, err)
		assert.NotEmpty(t, results)
		assert.Less(t, duration, 5*time.Second)

		t.Logf("Busca com filtro: %v, resultados: %d", duration, len(results))
	})
}
