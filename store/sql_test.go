package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/luma-sys/go-db-store/enum"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

type TestSQLEntity struct {
	ID        int       `db:"id" json:"id"`
	Name      string    `db:"name" json:"name"`
	Age       int       `db:"age" json:"age"`
	Active    bool      `db:"active" json:"active"`
	Score     float64   `db:"score" json:"score"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
}

type TestSQLEntityWithoutTimestamps struct {
	ID   int    `db:"id" json:"id"`
	Name string `db:"name" json:"name"`
}

type TestSQLEntityWithIgnoredField struct {
	ID      int    `db:"id" json:"id"`
	Name    string `db:"name" json:"name"`
	Ignored string `db:"-" json:"-"`
}

func setupSQLDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, errors.New("erro ao abrir conexão com SQLite: " + err.Error())
	}

	_, err = db.Exec(`
		CREATE TABLE test_entities (
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			age INTEGER DEFAULT 0,
			active BOOLEAN DEFAULT false,
			score REAL DEFAULT 0.0,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		return nil, errors.New("erro ao criar tabela: " + err.Error())
	}

	return db, nil
}

func setupSQLDBWithoutTimestamps() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, errors.New("erro ao abrir conexão com SQLite: " + err.Error())
	}

	_, err = db.Exec(`
		CREATE TABLE simple_entities (
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL
		);
	`)
	if err != nil {
		return nil, errors.New("erro ao criar tabela: " + err.Error())
	}

	return db, nil
}

// ==================== TESTES SAVE ====================

func TestSQLSave(t *testing.T) {
	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverSqlite, "test_entities", "id", true)
	ctx := context.Background()

	tests := []struct {
		name    string
		input   *TestSQLEntity
		check   func(*testing.T, *TestSQLEntity)
		wantErr bool
	}{
		{
			name: "deve salvar registro com todos os campos",
			input: &TestSQLEntity{
				Name:   "João Silva",
				Age:    30,
				Active: true,
				Score:  95.5,
			},
			check: func(t *testing.T, result *TestSQLEntity) {
				assert.NotZero(t, result.ID)
				assert.Equal(t, "João Silva", result.Name)
				assert.Equal(t, 30, result.Age)
				assert.True(t, result.Active)
				assert.Equal(t, 95.5, result.Score)
			},
		},
		{
			name: "deve gerar ID automaticamente",
			input: &TestSQLEntity{
				Name: "Maria Santos",
				Age:  25,
			},
			check: func(t *testing.T, result *TestSQLEntity) {
				assert.NotZero(t, result.ID)
				assert.Greater(t, result.ID, 0)
			},
		},
		{
			name: "deve salvar registro com campos mínimos",
			input: &TestSQLEntity{
				Name: "Campos Mínimos",
			},
			check: func(t *testing.T, result *TestSQLEntity) {
				assert.NotZero(t, result.ID)
				assert.Equal(t, "Campos Mínimos", result.Name)
				assert.Zero(t, result.Age)
				assert.False(t, result.Active)
			},
		},
		{
			name: "deve salvar registro com string vazia",
			input: &TestSQLEntity{
				Name: "",
			},
			check: func(t *testing.T, result *TestSQLEntity) {
				assert.NotZero(t, result.ID)
				assert.Empty(t, result.Name)
			},
		},
		{
			name: "deve salvar registro com valores negativos",
			input: &TestSQLEntity{
				Name:  "Valores Negativos",
				Age:   -1,
				Score: -50.5,
			},
			check: func(t *testing.T, result *TestSQLEntity) {
				assert.Equal(t, -1, result.Age)
				assert.Equal(t, -50.5, result.Score)
			},
		},
		{
			name: "deve salvar registro com valores zero",
			input: &TestSQLEntity{
				Name:   "Valores Zero",
				Age:    0,
				Score:  0.0,
				Active: false,
			},
			check: func(t *testing.T, result *TestSQLEntity) {
				assert.Zero(t, result.Age)
				assert.Zero(t, result.Score)
				assert.False(t, result.Active)
			},
		},
		{
			name: "deve salvar registro com valores grandes",
			input: &TestSQLEntity{
				Name:  "Valores Grandes",
				Age:   2147483647,
				Score: 1.7976931348623157e+100,
			},
			check: func(t *testing.T, result *TestSQLEntity) {
				assert.Equal(t, 2147483647, result.Age)
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
			found, err := store.FindById(ctx, result.ID)
			assert.NoError(t, err)
			assert.Equal(t, result.ID, found.ID)
		})
	}
}

func TestSQLSave_WithoutAutoincrement(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE manual_id_entities (
			id INTEGER NOT NULL PRIMARY KEY,
			name TEXT NOT NULL
		);
	`)
	if err != nil {
		t.Fatal(err)
	}

	store := NewSQLStore[TestSQLEntityWithoutTimestamps](db, enum.DatabaseDriverSqlite, "manual_id_entities", "id", false)
	ctx := context.Background()

	entity := &TestSQLEntityWithoutTimestamps{
		ID:   100,
		Name: "ID Manual",
	}

	result, err := store.Save(ctx, entity)
	assert.NoError(t, err)
	assert.Equal(t, 100, result.ID)

	found, err := store.FindById(ctx, 100)
	assert.NoError(t, err)
	assert.Equal(t, "ID Manual", found.Name)
}

func TestSQLSave_IgnoredFields(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE ignored_field_entities (
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL
		);
	`)
	if err != nil {
		t.Fatal(err)
	}

	store := NewSQLStore[TestSQLEntityWithIgnoredField](db, enum.DatabaseDriverSqlite, "ignored_field_entities", "id", true)
	ctx := context.Background()

	entity := &TestSQLEntityWithIgnoredField{
		Name:    "Com Campo Ignorado",
		Ignored: "Este campo não deve ser salvo",
	}

	result, err := store.Save(ctx, entity)
	assert.NoError(t, err)
	assert.NotZero(t, result.ID)
}

// ==================== TESTES SAVE MANY ====================

func TestSQLSaveMany(t *testing.T) {
	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverSqlite, "test_entities", "id", true)
	ctx := context.Background()

	tests := []struct {
		name    string
		input   []TestSQLEntity
		check   func(*testing.T, *InsertManyResult)
		wantErr bool
	}{
		{
			name: "deve salvar múltiplos registros",
			input: []TestSQLEntity{
				{Name: "João", Age: 25},
				{Name: "Maria", Age: 30},
				{Name: "Pedro", Age: 35},
			},
			check: func(t *testing.T, result *InsertManyResult) {
				assert.Equal(t, 3, len(result.InsertedIDs))
			},
		},
		{
			name: "deve salvar um único registro",
			input: []TestSQLEntity{
				{Name: "Único", Age: 40},
			},
			check: func(t *testing.T, result *InsertManyResult) {
				assert.Equal(t, 1, len(result.InsertedIDs))
			},
		},
		{
			name:  "deve retornar nil para slice vazio",
			input: []TestSQLEntity{},
			check: func(t *testing.T, result *InsertManyResult) {
				assert.Nil(t, result)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Limpa a tabela
			db.Exec("DELETE FROM test_entities")

			result, err := store.SaveMany(ctx, tt.input)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			if tt.check != nil {
				tt.check(t, result)
			}

			// Verifica contagem
			if len(tt.input) > 0 {
				count, _ := store.Count(ctx, map[string]any{})
				assert.Equal(t, int64(len(tt.input)), *count)
			}
		})
	}
}

// ==================== TESTES SAVE MANY NOT ORDERED ====================

func TestSQLSaveManyNotOrdered(t *testing.T) {
	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverSqlite, "test_entities", "id", true)
	ctx := context.Background()

	entities := []TestSQLEntity{
		{Name: "Doc 1"},
		{Name: "Doc 2"},
	}

	result, err := store.SaveManyNotOrdered(ctx, entities)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "not implemented")
}

// ==================== TESTES FIND BY ID ====================

func TestSQLFindById(t *testing.T) {
	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverSqlite, "test_entities", "id", true)
	ctx := context.Background()

	// Setup: salva registros de teste
	testDoc := &TestSQLEntity{
		Name:   "Documento Teste",
		Age:    25,
		Active: true,
		Score:  88.5,
	}
	saved, _ := store.Save(ctx, testDoc)

	tests := []struct {
		name    string
		id      any
		check   func(*testing.T, *TestSQLEntity)
		wantErr bool
	}{
		{
			name: "deve encontrar registro existente",
			id:   saved.ID,
			check: func(t *testing.T, result *TestSQLEntity) {
				assert.Equal(t, saved.ID, result.ID)
				assert.Equal(t, "Documento Teste", result.Name)
				assert.Equal(t, 25, result.Age)
				assert.True(t, result.Active)
				assert.Equal(t, 88.5, result.Score)
			},
		},
		{
			name:    "deve retornar erro para ID inexistente",
			id:      99999,
			wantErr: true,
		},
		{
			name:    "deve retornar erro para ID zero",
			id:      0,
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

// ==================== TESTES FIND ALL ====================

func TestSQLFindAll(t *testing.T) {
	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverSqlite, "test_entities", "id", true)
	ctx := context.Background()

	// Setup: salva registros de teste
	testDocs := []TestSQLEntity{
		{Name: "João", Age: 25, Active: true, Score: 80},
		{Name: "Maria", Age: 30, Active: true, Score: 90},
		{Name: "Pedro", Age: 35, Active: false, Score: 70},
		{Name: "Ana", Age: 28, Active: true, Score: 85},
		{Name: "Carlos", Age: 40, Active: false, Score: 75},
	}
	for _, doc := range testDocs {
		_, _ = store.Save(ctx, &doc)
	}

	tests := []struct {
		name    string
		filter  map[string]any
		opts    FindOptions
		wantLen int
		check   func(*testing.T, []TestSQLEntity)
		wantErr bool
	}{
		{
			name:    "deve retornar todos os registros sem filtro",
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
			name:    "deve filtrar por campo booleano true",
			filter:  map[string]any{"active": true},
			opts:    FindOptions{},
			wantLen: 3,
			check: func(t *testing.T, results []TestSQLEntity) {
				for _, r := range results {
					assert.True(t, r.Active)
				}
			},
		},
		{
			name:    "deve filtrar por campo booleano false",
			filter:  map[string]any{"active": false},
			opts:    FindOptions{},
			wantLen: 2,
			check: func(t *testing.T, results []TestSQLEntity) {
				for _, r := range results {
					assert.False(t, r.Active)
				}
			},
		},
		{
			name:    "deve filtrar por campo string",
			filter:  map[string]any{"name": "João"},
			opts:    FindOptions{},
			wantLen: 1,
			check: func(t *testing.T, results []TestSQLEntity) {
				assert.Equal(t, "João", results[0].Name)
			},
		},
		{
			name:    "deve usar operador __gt",
			filter:  map[string]any{"age__gt": 30},
			opts:    FindOptions{},
			wantLen: 2,
			check: func(t *testing.T, results []TestSQLEntity) {
				for _, r := range results {
					assert.Greater(t, r.Age, 30)
				}
			},
		},
		{
			name:    "deve usar operador __gte",
			filter:  map[string]any{"age__gte": 30},
			opts:    FindOptions{},
			wantLen: 3,
			check: func(t *testing.T, results []TestSQLEntity) {
				for _, r := range results {
					assert.GreaterOrEqual(t, r.Age, 30)
				}
			},
		},
		{
			name:    "deve usar operador __lt",
			filter:  map[string]any{"age__lt": 30},
			opts:    FindOptions{},
			wantLen: 2,
			check: func(t *testing.T, results []TestSQLEntity) {
				for _, r := range results {
					assert.Less(t, r.Age, 30)
				}
			},
		},
		{
			name:    "deve usar operador __lte",
			filter:  map[string]any{"age__lte": 30},
			opts:    FindOptions{},
			wantLen: 3,
			check: func(t *testing.T, results []TestSQLEntity) {
				for _, r := range results {
					assert.LessOrEqual(t, r.Age, 30)
				}
			},
		},
		{
			name:    "deve usar operador __like",
			filter:  map[string]any{"name__like": "%a%"},
			opts:    FindOptions{},
			wantLen: 3, // Maria, Ana, Carlos
			check: func(t *testing.T, results []TestSQLEntity) {
				for _, r := range results {
					assert.Contains(t, r.Name, "a")
				}
			},
		},
		{
			name:    "deve usar operador __like no início",
			filter:  map[string]any{"name__like": "M%"},
			opts:    FindOptions{},
			wantLen: 1,
			check: func(t *testing.T, results []TestSQLEntity) {
				assert.Equal(t, "Maria", results[0].Name)
			},
		},
		{
			name:    "deve usar operador __not_like",
			filter:  map[string]any{"name__not_like": "%a%"},
			opts:    FindOptions{},
			wantLen: 2, // João, Pedro
		},
		{
			name:    "deve usar operador __not",
			filter:  map[string]any{"name__not": "João"},
			opts:    FindOptions{},
			wantLen: 4,
			check: func(t *testing.T, results []TestSQLEntity) {
				for _, r := range results {
					assert.NotEqual(t, "João", r.Name)
				}
			},
		},
		{
			name:    "deve usar operador __in com []int",
			filter:  map[string]any{"age__in": []int{25, 30}},
			opts:    FindOptions{},
			wantLen: 2,
			check: func(t *testing.T, results []TestSQLEntity) {
				for _, r := range results {
					assert.True(t, r.Age == 25 || r.Age == 30)
				}
			},
		},
		{
			name:    "deve usar operador __in com []string",
			filter:  map[string]any{"name__in": []string{"João", "Maria"}},
			opts:    FindOptions{},
			wantLen: 2,
		},
		{
			name:    "deve combinar múltiplos filtros",
			filter:  map[string]any{"active": true, "age__gte": 28},
			opts:    FindOptions{},
			wantLen: 2, // Maria (30, active) e Ana (28, active)
			check: func(t *testing.T, results []TestSQLEntity) {
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
			name:    "deve retornar vazio quando filtro não encontra",
			filter:  map[string]any{"name": "NaoExiste"},
			opts:    FindOptions{},
			wantLen: 0,
		},
		{
			name:    "deve retornar vazio para página além dos resultados",
			filter:  nil,
			opts:    FindOptions{Page: 100, Limit: 10},
			wantLen: 0,
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
			assert.Equal(t, tt.wantLen, len(results))

			if tt.check != nil {
				tt.check(t, results)
			}
		})
	}
}

func TestSQLFindAll_IsNullOperators(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE nullable_entities (
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
			name TEXT,
			age INTEGER
		);
		INSERT INTO nullable_entities (name, age) VALUES ('João', 25);
		INSERT INTO nullable_entities (name, age) VALUES (NULL, 30);
		INSERT INTO nullable_entities (name, age) VALUES ('Maria', NULL);
	`)
	if err != nil {
		t.Fatal(err)
	}

	store := NewSQLStore[TestSQLEntityWithoutTimestamps](db, enum.DatabaseDriverSqlite, "nullable_entities", "id", true)
	ctx := context.Background()

	t.Run("deve usar operador __is_null", func(t *testing.T) {
		results, err := store.FindAll(ctx, map[string]any{"name__is_null": true}, FindOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(results))
	})

	t.Run("deve usar operador __is_not_null", func(t *testing.T) {
		results, err := store.FindAll(ctx, map[string]any{"name__is_not_null": true}, FindOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(results))
	})
}

// ==================== TESTES COUNT ====================

func TestSQLCount(t *testing.T) {
	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverSqlite, "test_entities", "id", true)
	ctx := context.Background()

	// Setup
	testDocs := []TestSQLEntity{
		{Name: "João", Age: 25, Active: true},
		{Name: "Maria", Age: 30, Active: true},
		{Name: "Pedro", Age: 35, Active: false},
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
			name:      "deve contar todos os registros",
			filter:    map[string]any{},
			wantCount: 3,
		},
		{
			name:      "deve contar com filtro booleano",
			filter:    map[string]any{"active": true},
			wantCount: 2,
		},
		{
			name:      "deve contar com operador __gt",
			filter:    map[string]any{"age__gt": 25},
			wantCount: 2,
		},
		{
			name:      "deve retornar zero quando não encontra",
			filter:    map[string]any{"name": "NaoExiste"},
			wantCount: 0,
		},
		{
			name:      "deve contar com múltiplos filtros",
			filter:    map[string]any{"active": true, "age__gte": 30},
			wantCount: 1,
		},
		{
			name:      "deve contar com operador __in",
			filter:    map[string]any{"name__in": []string{"João", "Maria"}},
			wantCount: 2,
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

func TestSQLHas(t *testing.T) {
	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverSqlite, "test_entities", "id", true)
	ctx := context.Background()

	// Setup
	saved, _ := store.Save(ctx, &TestSQLEntity{Name: "Existe"})

	tests := []struct {
		name string
		id   any
		want bool
	}{
		{
			name: "deve retornar true para registro existente",
			id:   saved.ID,
			want: true,
		},
		{
			name: "deve retornar false para registro inexistente",
			id:   99999,
			want: false,
		},
		{
			name: "deve retornar false para ID zero",
			id:   0,
			want: false,
		},
		{
			name: "deve retornar false para ID negativo",
			id:   -1,
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

func TestSQLUpdate(t *testing.T) {
	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverSqlite, "test_entities", "id", true)
	ctx := context.Background()

	tests := []struct {
		name    string
		setup   func() *TestSQLEntity
		update  func(*TestSQLEntity) *TestSQLEntity
		check   func(*testing.T, *TestSQLEntity)
		wantErr bool
	}{
		{
			name: "deve atualizar campo string",
			setup: func() *TestSQLEntity {
				doc := &TestSQLEntity{Name: "Original", Age: 25}
				store.Save(ctx, doc)
				return doc
			},
			update: func(e *TestSQLEntity) *TestSQLEntity {
				e.Name = "Atualizado"
				return e
			},
			check: func(t *testing.T, result *TestSQLEntity) {
				assert.Equal(t, "Atualizado", result.Name)
				assert.Equal(t, 25, result.Age)
			},
		},
		{
			name: "deve atualizar campo numérico",
			setup: func() *TestSQLEntity {
				doc := &TestSQLEntity{Name: "Teste", Age: 25, Score: 80}
				store.Save(ctx, doc)
				return doc
			},
			update: func(e *TestSQLEntity) *TestSQLEntity {
				e.Age = 30
				e.Score = 95.5
				return e
			},
			check: func(t *testing.T, result *TestSQLEntity) {
				found, _ := store.FindById(ctx, result.ID)
				assert.Equal(t, 30, found.Age)
				assert.Equal(t, 95.5, found.Score)
			},
		},
		{
			name: "deve atualizar campo booleano",
			setup: func() *TestSQLEntity {
				doc := &TestSQLEntity{Name: "Teste", Active: false}
				store.Save(ctx, doc)
				return doc
			},
			update: func(e *TestSQLEntity) *TestSQLEntity {
				e.Active = true
				return e
			},
			check: func(t *testing.T, result *TestSQLEntity) {
				found, _ := store.FindById(ctx, result.ID)
				assert.True(t, found.Active)
			},
		},
		{
			name: "deve atualizar UpdatedAt automaticamente",
			setup: func() *TestSQLEntity {
				doc := &TestSQLEntity{Name: "Teste"}
				store.Save(ctx, doc)
				time.Sleep(10 * time.Millisecond)
				return doc
			},
			update: func(e *TestSQLEntity) *TestSQLEntity {
				e.Name = "Atualizado"
				return e
			},
			check: func(t *testing.T, result *TestSQLEntity) {
				assert.True(t, time.Since(result.UpdatedAt) < time.Minute)
			},
		},
		{
			name: "deve atualizar múltiplos campos",
			setup: func() *TestSQLEntity {
				doc := &TestSQLEntity{Name: "Original", Age: 20, Score: 50, Active: false}
				store.Save(ctx, doc)
				return doc
			},
			update: func(e *TestSQLEntity) *TestSQLEntity {
				e.Name = "Atualizado"
				e.Age = 30
				e.Score = 100
				e.Active = true
				return e
			},
			check: func(t *testing.T, result *TestSQLEntity) {
				found, _ := store.FindById(ctx, result.ID)
				assert.Equal(t, "Atualizado", found.Name)
				assert.Equal(t, 30, found.Age)
				assert.Equal(t, 100.0, found.Score)
				assert.True(t, found.Active)
			},
		},
		{
			name: "deve retornar erro para registro inexistente",
			setup: func() *TestSQLEntity {
				return &TestSQLEntity{ID: 99999, Name: "Teste"}
			},
			update: func(e *TestSQLEntity) *TestSQLEntity {
				return e
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Limpa tabela
			db.Exec("DELETE FROM test_entities")

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

func TestSQLUpdateMany(t *testing.T) {
	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverSqlite, "test_entities", "id", true)
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
			name: "deve atualizar um único registro",
			setup: func() {
				store.Save(ctx, &TestSQLEntity{Name: "Original", Age: 25})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"name": "Original"},
					Fields: map[string]any{"name": "Atualizado"},
				},
			},
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Equal(t, int64(1), result.ModifiedCount)

				records, _ := store.FindAll(ctx, map[string]any{"name": "Atualizado"}, FindOptions{})
				assert.Equal(t, 1, len(records))
			},
		},
		{
			name: "deve atualizar múltiplos registros com filtros diferentes",
			setup: func() {
				store.Save(ctx, &TestSQLEntity{Name: "João", Age: 25})
				store.Save(ctx, &TestSQLEntity{Name: "Maria", Age: 30})
				store.Save(ctx, &TestSQLEntity{Name: "Pedro", Age: 35})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"name": "João"},
					Fields: map[string]any{"name": "João Atualizado"},
				},
				{
					Filter: map[string]any{"name": "Maria"},
					Fields: map[string]any{"name": "Maria Atualizada"},
				},
			},
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Equal(t, int64(2), result.ModifiedCount)

				records, _ := store.FindAll(ctx, map[string]any{"name": "Pedro"}, FindOptions{})
				assert.Equal(t, 1, len(records))
			},
		},
		{
			name: "deve atualizar vários registros com mesmo filtro",
			setup: func() {
				store.Save(ctx, &TestSQLEntity{Name: "Grupo A", Active: true})
				store.Save(ctx, &TestSQLEntity{Name: "Grupo A", Active: true})
				store.Save(ctx, &TestSQLEntity{Name: "Grupo B", Active: false})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"active": true},
					Fields: map[string]any{"name": "Grupo A Atualizado"},
				},
			},
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Equal(t, int64(2), result.ModifiedCount)

				records, _ := store.FindAll(ctx, map[string]any{"name": "Grupo A Atualizado"}, FindOptions{})
				assert.Equal(t, 2, len(records))
			},
		},
		{
			name: "deve atualizar updated_at automaticamente",
			setup: func() {
				store.Save(ctx, &TestSQLEntity{Name: "Original"})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"name": "Original"},
					Fields: map[string]any{"name": "Atualizado"},
				},
			},
			check: func(t *testing.T, result *BulkWriteResult) {
				records, _ := store.FindAll(ctx, map[string]any{"name": "Atualizado"}, FindOptions{})
				assert.True(t, time.Since(records[0].UpdatedAt) < time.Minute)
			},
		},
		{
			name: "deve usar operador __like no filtro",
			setup: func() {
				store.Save(ctx, &TestSQLEntity{Name: "João Silva", Age: 25})
				store.Save(ctx, &TestSQLEntity{Name: "João Santos", Age: 30})
				store.Save(ctx, &TestSQLEntity{Name: "Maria Silva", Age: 35})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"name__like": "João%"},
					Fields: map[string]any{"active": true},
				},
			},
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Equal(t, int64(2), result.ModifiedCount)
			},
		},
		{
			name: "deve usar operador __in no filtro",
			setup: func() {
				store.Save(ctx, &TestSQLEntity{Name: "Registro 1", Age: 25})
				store.Save(ctx, &TestSQLEntity{Name: "Registro 2", Age: 30})
				store.Save(ctx, &TestSQLEntity{Name: "Registro 3", Age: 35})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"age__in": []int{25, 35}},
					Fields: map[string]any{"active": true},
				},
			},
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Equal(t, int64(2), result.ModifiedCount)
			},
		},
		{
			name: "deve usar operador __gte no filtro",
			setup: func() {
				store.Save(ctx, &TestSQLEntity{Name: "Jovem", Age: 20})
				store.Save(ctx, &TestSQLEntity{Name: "Adulto", Age: 30})
				store.Save(ctx, &TestSQLEntity{Name: "Senior", Age: 50})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"age__gte": 30},
					Fields: map[string]any{"active": true},
				},
			},
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Equal(t, int64(2), result.ModifiedCount)
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
				store.Save(ctx, &TestSQLEntity{Name: "Original"})
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
			name: "deve retornar erro quando campos vazios",
			setup: func() {
				store.Save(ctx, &TestSQLEntity{Name: "Original"})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"name": "Original"},
					Fields: map[string]any{},
				},
			},
			wantErr: true,
			errMsg:  "campos para atualização são obrigatórios para update 0",
		},
		{
			name: "deve retornar zero quando filtro não encontra",
			setup: func() {
				store.Save(ctx, &TestSQLEntity{Name: "Original"})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"name": "NaoExiste"},
					Fields: map[string]any{"name": "Atualizado"},
				},
			},
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Equal(t, int64(0), result.ModifiedCount)
			},
		},
		{
			name: "deve fazer rollback em caso de erro no meio da transação",
			setup: func() {
				store.Save(ctx, &TestSQLEntity{Name: "Original 1"})
				store.Save(ctx, &TestSQLEntity{Name: "Original 2"})
			},
			input: []EntityFieldsToUpdate{
				{
					Filter: map[string]any{"name": "Original 1"},
					Fields: map[string]any{"name": "Atualizado 1"},
				},
				{
					Filter: map[string]any{},
					Fields: map[string]any{"name": "Atualizado 2"},
				},
			},
			wantErr: true,
			errMsg:  "filtro é obrigatório para update 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db.Exec("DELETE FROM test_entities")
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

func TestSQLUpsert(t *testing.T) {
	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverSqlite, "test_entities", "id", true)
	ctx := context.Background()

	tests := []struct {
		name    string
		setup   func()
		input   *TestSQLEntity
		filters []StoreUpsertFilter
		check   func(*testing.T, *UpdateResult)
		wantErr bool
	}{
		{
			name:  "deve inserir novo registro quando não existe",
			setup: func() {},
			input: &TestSQLEntity{
				Name:   "Novo Registro",
				Age:    25,
				Active: true,
			},
			filters: nil,
			check: func(t *testing.T, result *UpdateResult) {
				assert.Equal(t, int64(1), result.UpsertedCount)

				count, _ := store.Count(ctx, map[string]any{})
				assert.Equal(t, int64(1), *count)
			},
		},
		{
			name: "deve atualizar registro existente (SQLite usa INSERT OR REPLACE)",
			setup: func() {
				store.Save(ctx, &TestSQLEntity{Name: "Original", Age: 25})
			},
			input: &TestSQLEntity{
				ID:   1,
				Name: "Atualizado",
				Age:  30,
			},
			filters: nil,
			check: func(t *testing.T, result *UpdateResult) {
				assert.Equal(t, int64(1), result.UpsertedCount)

				found, _ := store.FindById(ctx, 1)
				assert.Equal(t, "Atualizado", found.Name)
				assert.Equal(t, 30, found.Age)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db.Exec("DELETE FROM test_entities")
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

func TestSQLUpsert_UnsupportedDriver(t *testing.T) {
	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Cria store com driver não suportado
	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverOracle, "test_entities", "id", true)
	ctx := context.Background()

	_, err = store.Upsert(ctx, &TestSQLEntity{Name: "Teste"}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported database driver")
}

// ==================== TESTES UPSERT MANY ====================

func TestSQLUpsertMany(t *testing.T) {
	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverSqlite, "test_entities", "id", true)
	ctx := context.Background()

	tests := []struct {
		name    string
		setup   func()
		input   []TestSQLEntity
		filters []StoreUpsertFilter
		check   func(*testing.T, *BulkWriteResult)
		wantErr bool
	}{
		{
			name:  "deve inserir múltiplos novos registros",
			setup: func() {},
			input: []TestSQLEntity{
				{Name: "Doc 1", Age: 25},
				{Name: "Doc 2", Age: 30},
				{Name: "Doc 3", Age: 35},
			},
			filters: nil,
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Equal(t, int64(3), result.UpsertedCount)

				count, _ := store.Count(ctx, map[string]any{})
				assert.Equal(t, int64(3), *count)
			},
		},
		{
			name:    "deve retornar nil para slice vazio",
			setup:   func() {},
			input:   []TestSQLEntity{},
			filters: nil,
			check: func(t *testing.T, result *BulkWriteResult) {
				assert.Nil(t, result)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db.Exec("DELETE FROM test_entities")
			tt.setup()

			result, err := store.UpsertMany(ctx, tt.input, tt.filters)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			if tt.check != nil {
				tt.check(t, result)
			}
		})
	}
}

// ==================== TESTES DELETE ====================

func TestSQLDelete(t *testing.T) {
	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverSqlite, "test_entities", "id", true)
	ctx := context.Background()

	tests := []struct {
		name    string
		setup   func() int
		check   func(*testing.T, int)
		wantErr bool
	}{
		{
			name: "deve deletar registro existente",
			setup: func() int {
				saved, _ := store.Save(ctx, &TestSQLEntity{Name: "Para Deletar"})
				return saved.ID
			},
			check: func(t *testing.T, id int) {
				exists := store.Has(ctx, id)
				assert.False(t, exists)
			},
		},
		{
			name: "não deve retornar erro para registro inexistente",
			setup: func() int {
				return 99999
			},
			check: func(t *testing.T, id int) {
				// SQLite não retorna erro para DELETE de registro inexistente
			},
		},
		{
			name: "deve manter outros registros intactos",
			setup: func() int {
				store.Save(ctx, &TestSQLEntity{Name: "Manter 1"})
				toDelete, _ := store.Save(ctx, &TestSQLEntity{Name: "Deletar"})
				store.Save(ctx, &TestSQLEntity{Name: "Manter 2"})
				return toDelete.ID
			},
			check: func(t *testing.T, id int) {
				assert.False(t, store.Has(ctx, id))

				count, _ := store.Count(ctx, map[string]any{})
				assert.Equal(t, int64(2), *count)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db.Exec("DELETE FROM test_entities")

			id := tt.setup()
			err := store.Delete(ctx, id)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			if tt.check != nil {
				tt.check(t, id)
			}
		})
	}
}

// ==================== TESTES DELETE MANY ====================

func TestSQLDeleteMany(t *testing.T) {
	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverSqlite, "test_entities", "id", true)
	ctx := context.Background()

	tests := []struct {
		name    string
		setup   func()
		filter  map[string]any
		check   func(*testing.T, *DeleteResult)
		wantErr bool
	}{
		{
			name: "deve deletar múltiplos registros",
			setup: func() {
				store.Save(ctx, &TestSQLEntity{Name: "Doc", Active: true})
				store.Save(ctx, &TestSQLEntity{Name: "Doc", Active: true})
				store.Save(ctx, &TestSQLEntity{Name: "Doc", Active: false})
			},
			filter: map[string]any{"active": true},
			check: func(t *testing.T, result *DeleteResult) {
				assert.Equal(t, int64(2), result.DeletedCount)

				count, _ := store.Count(ctx, map[string]any{})
				assert.Equal(t, int64(1), *count)
			},
		},
		{
			name: "deve usar operadores no filtro",
			setup: func() {
				store.Save(ctx, &TestSQLEntity{Name: "Doc", Age: 20})
				store.Save(ctx, &TestSQLEntity{Name: "Doc", Age: 30})
				store.Save(ctx, &TestSQLEntity{Name: "Doc", Age: 40})
			},
			filter: map[string]any{"age__gte": 30},
			check: func(t *testing.T, result *DeleteResult) {
				assert.Equal(t, int64(2), result.DeletedCount)
			},
		},
		{
			name: "deve usar operador __in",
			setup: func() {
				store.Save(ctx, &TestSQLEntity{Name: "Doc 1", Age: 25})
				store.Save(ctx, &TestSQLEntity{Name: "Doc 2", Age: 30})
				store.Save(ctx, &TestSQLEntity{Name: "Doc 3", Age: 35})
			},
			filter: map[string]any{"age__in": []int{25, 35}},
			check: func(t *testing.T, result *DeleteResult) {
				assert.Equal(t, int64(2), result.DeletedCount)

				count, _ := store.Count(ctx, map[string]any{})
				assert.Equal(t, int64(1), *count)
			},
		},
		{
			name: "deve usar operador __like",
			setup: func() {
				store.Save(ctx, &TestSQLEntity{Name: "João Silva"})
				store.Save(ctx, &TestSQLEntity{Name: "João Santos"})
				store.Save(ctx, &TestSQLEntity{Name: "Maria Silva"})
			},
			filter: map[string]any{"name__like": "João%"},
			check: func(t *testing.T, result *DeleteResult) {
				assert.Equal(t, int64(2), result.DeletedCount)
			},
		},
		{
			name: "deve retornar zero quando não encontra",
			setup: func() {
				store.Save(ctx, &TestSQLEntity{Name: "Doc"})
			},
			filter: map[string]any{"name": "NaoExiste"},
			check: func(t *testing.T, result *DeleteResult) {
				assert.Equal(t, int64(0), result.DeletedCount)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db.Exec("DELETE FROM test_entities")
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

func TestSQLWithTransaction(t *testing.T) {
	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverSqlite, "test_entities", "id", true)
	ctx := context.Background()

	t.Run("deve executar operações em transação com sucesso", func(t *testing.T) {
		result, err := store.WithTransaction(ctx, func(txCtx TransactionContext) (any, error) {
			return "success", nil
		})

		assert.NoError(t, err)
		assert.Equal(t, "success", result)
	})

	t.Run("deve fazer rollback em caso de erro", func(t *testing.T) {
		_, err := store.WithTransaction(ctx, func(txCtx TransactionContext) (any, error) {
			return nil, fmt.Errorf("erro simulado")
		})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "erro simulado")
	})

	t.Run("deve permitir operações SQL dentro da transação", func(t *testing.T) {
		db.Exec("DELETE FROM test_entities")

		result, err := store.WithTransaction(ctx, func(txCtx TransactionContext) (any, error) {
			tx, ok := txCtx.(*sql.Tx)
			if !ok {
				return nil, fmt.Errorf("contexto inválido")
			}

			_, err := tx.Exec("INSERT INTO test_entities (name, age, active, score) VALUES (?, ?, ?, ?)",
				"Transação", 25, true, 80.0)
			if err != nil {
				return nil, err
			}

			return "inserted", nil
		})

		assert.NoError(t, err)
		assert.Equal(t, "inserted", result)

		// Verifica se foi commitado
		count, _ := store.Count(ctx, map[string]any{})
		assert.Equal(t, int64(1), *count)
	})

	t.Run("deve fazer rollback quando transação falha", func(t *testing.T) {
		db.Exec("DELETE FROM test_entities")

		_, err := store.WithTransaction(ctx, func(txCtx TransactionContext) (any, error) {
			tx, ok := txCtx.(*sql.Tx)
			if !ok {
				return nil, fmt.Errorf("contexto inválido")
			}

			_, err := tx.Exec("INSERT INTO test_entities (name, age, active, score) VALUES (?, ?, ?, ?)",
				"Vai Falhar", 25, true, 80.0)
			if err != nil {
				return nil, err
			}

			return nil, fmt.Errorf("erro forçado")
		})

		assert.Error(t, err)

		// Verifica se foi feito rollback
		count, _ := store.Count(ctx, map[string]any{})
		assert.Equal(t, int64(0), *count)
	})
}

// ==================== TESTES BUILD WHERE CLAUSE ====================

func TestSQLBuildWhereClause(t *testing.T) {
	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverSqlite, "test_entities", "id", true).(*SQLStore[TestSQLEntity])

	tests := []struct {
		name          string
		filters       map[string]any
		wantClause    string
		wantValuesLen int
	}{
		{
			name:          "deve retornar vazio para filtro nil",
			filters:       nil,
			wantClause:    "",
			wantValuesLen: 0,
		},
		{
			name:          "deve retornar vazio para filtro vazio",
			filters:       map[string]any{},
			wantClause:    "",
			wantValuesLen: 0,
		},
		{
			name:          "deve construir cláusula simples de igualdade",
			filters:       map[string]any{"name": "João"},
			wantClause:    " WHERE name = ?",
			wantValuesLen: 1,
		},
		{
			name:          "deve construir cláusula com operador __gt",
			filters:       map[string]any{"age__gt": 30},
			wantClause:    " WHERE age > ?",
			wantValuesLen: 1,
		},
		{
			name:          "deve construir cláusula com operador __gte",
			filters:       map[string]any{"age__gte": 30},
			wantClause:    " WHERE age >= ?",
			wantValuesLen: 1,
		},
		{
			name:          "deve construir cláusula com operador __lt",
			filters:       map[string]any{"age__lt": 30},
			wantClause:    " WHERE age < ?",
			wantValuesLen: 1,
		},
		{
			name:          "deve construir cláusula com operador __lte",
			filters:       map[string]any{"age__lte": 30},
			wantClause:    " WHERE age <= ?",
			wantValuesLen: 1,
		},
		{
			name:          "deve construir cláusula com operador __like",
			filters:       map[string]any{"name__like": "%João%"},
			wantClause:    " WHERE name LIKE ?",
			wantValuesLen: 1,
		},
		{
			name:          "deve construir cláusula com operador __ilike",
			filters:       map[string]any{"name__ilike": "%joão%"},
			wantClause:    " WHERE name ILIKE ?",
			wantValuesLen: 1,
		},
		{
			name:          "deve construir cláusula com operador __not_like",
			filters:       map[string]any{"name__not_like": "%João%"},
			wantClause:    " WHERE name NOT LIKE ?",
			wantValuesLen: 1,
		},
		{
			name:          "deve construir cláusula com operador __not",
			filters:       map[string]any{"name__not": "João"},
			wantClause:    " WHERE name != ?",
			wantValuesLen: 1,
		},
		{
			name:          "deve construir cláusula com operador __is_null",
			filters:       map[string]any{"name__is_null": true},
			wantClause:    " WHERE name IS NULL",
			wantValuesLen: 0,
		},
		{
			name:          "deve construir cláusula com operador __is_not_null",
			filters:       map[string]any{"name__is_not_null": true},
			wantClause:    " WHERE name IS NOT NULL",
			wantValuesLen: 0,
		},
		{
			name:          "deve construir cláusula com operador __in ([]int)",
			filters:       map[string]any{"age__in": []int{25, 30, 35}},
			wantClause:    " WHERE age IN (?, ?, ?)",
			wantValuesLen: 3,
		},
		{
			name:          "deve construir cláusula com operador __in ([]string)",
			filters:       map[string]any{"name__in": []string{"João", "Maria"}},
			wantClause:    " WHERE name IN (?, ?)",
			wantValuesLen: 2,
		},
		{
			name:          "deve ordenar chaves alfabeticamente",
			filters:       map[string]any{"name": "João", "age": 30},
			wantClause:    " WHERE age = ? AND name = ?",
			wantValuesLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause, values := store.buildWhereClause(tt.filters)

			assert.Equal(t, tt.wantClause, clause)
			assert.Equal(t, tt.wantValuesLen, len(values))
		})
	}
}

// ==================== TESTES DE EDGE CASES ====================

func TestSQLEdgeCases(t *testing.T) {
	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverSqlite, "test_entities", "id", true)
	ctx := context.Background()

	t.Run("deve lidar com registros com campos especiais", func(t *testing.T) {
		db.Exec("DELETE FROM test_entities")

		doc := &TestSQLEntity{
			Name: "Nome com 'aspas' e \"aspas duplas\"",
		}

		saved, err := store.Save(ctx, doc)
		assert.NoError(t, err)

		found, err := store.FindById(ctx, saved.ID)
		assert.NoError(t, err)
		assert.Equal(t, saved.Name, found.Name)
	})

	t.Run("deve lidar com valores extremos", func(t *testing.T) {
		db.Exec("DELETE FROM test_entities")

		doc := &TestSQLEntity{
			Name:  "Extreme Values",
			Age:   2147483647,
			Score: 1.7976931348623157e+100,
		}

		saved, err := store.Save(ctx, doc)
		assert.NoError(t, err)

		found, err := store.FindById(ctx, saved.ID)
		assert.NoError(t, err)
		assert.Equal(t, doc.Age, found.Age)
	})

	t.Run("deve lidar com strings vazias em busca", func(t *testing.T) {
		db.Exec("DELETE FROM test_entities")

		store.Save(ctx, &TestSQLEntity{Name: ""})
		store.Save(ctx, &TestSQLEntity{Name: "Teste"})

		results, err := store.FindAll(ctx, map[string]any{"name": ""}, FindOptions{})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(results))
		assert.Empty(t, results[0].Name)
	})

	t.Run("deve lidar com operações em tabela vazia", func(t *testing.T) {
		db.Exec("DELETE FROM test_entities")

		count, err := store.Count(ctx, map[string]any{})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), *count)

		results, err := store.FindAll(ctx, nil, FindOptions{})
		assert.NoError(t, err)
		assert.Empty(t, results)

		exists := store.Has(ctx, 1)
		assert.False(t, exists)
	})

	t.Run("deve lidar com caracteres unicode", func(t *testing.T) {
		db.Exec("DELETE FROM test_entities")

		doc := &TestSQLEntity{
			Name: "日本語テスト 🎉 émojis ñ ç",
		}

		saved, err := store.Save(ctx, doc)
		assert.NoError(t, err)

		found, err := store.FindById(ctx, saved.ID)
		assert.NoError(t, err)
		assert.Equal(t, doc.Name, found.Name)
	})

	t.Run("deve lidar com filtro __in com slice vazio via reflection", func(t *testing.T) {
		db.Exec("DELETE FROM test_entities")

		store.Save(ctx, &TestSQLEntity{Name: "Test", Age: 25})

		// Slice de float64 (não tratado explicitamente)
		results, err := store.FindAll(ctx, map[string]any{"score__in": []float64{80.0, 90.0}}, FindOptions{})
		assert.NoError(t, err)
		assert.Empty(t, results)
	})
}

// ==================== TESTES DE PERFORMANCE ====================

func TestSQLPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Pulando testes de performance em modo curto")
	}

	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverSqlite, "test_entities", "id", true)
	ctx := context.Background()

	t.Run("deve inserir 1000 registros em batch eficientemente", func(t *testing.T) {
		db.Exec("DELETE FROM test_entities")

		entities := make([]TestSQLEntity, 1000)
		for i := 0; i < 1000; i++ {
			entities[i] = TestSQLEntity{
				Name:   fmt.Sprintf("Performance Test %d", i),
				Age:    i % 100,
				Active: i%2 == 0,
				Score:  float64(i) * 1.5,
			}
		}

		start := time.Now()
		result, err := store.SaveMany(ctx, entities)
		duration := time.Since(start)

		assert.NoError(t, err)
		assert.Equal(t, 1000, len(result.InsertedIDs))
		assert.Less(t, duration, 30*time.Second)

		t.Logf("Inserção de 1000 registros: %v", duration)
	})

	t.Run("deve buscar com filtro eficientemente", func(t *testing.T) {
		start := time.Now()
		results, err := store.FindAll(ctx, map[string]any{"age__gte": 50}, FindOptions{})
		duration := time.Since(start)

		assert.NoError(t, err)
		assert.NotEmpty(t, results)
		assert.Less(t, duration, 5*time.Second)

		t.Logf("Busca com filtro: %v, resultados: %d", duration, len(results))
	})

	t.Run("deve contar registros eficientemente", func(t *testing.T) {
		start := time.Now()
		count, err := store.Count(ctx, map[string]any{"active": true})
		duration := time.Since(start)

		assert.NoError(t, err)
		assert.Greater(t, *count, int64(0))
		assert.Less(t, duration, 1*time.Second)

		t.Logf("Contagem: %v, total: %d", duration, *count)
	})
}

// ==================== TESTES DE CONVERSÃO DE TIPOS ====================

func TestSQLTypeConversion(t *testing.T) {
	db, err := setupSQLDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQLStore[TestSQLEntity](db, enum.DatabaseDriverSqlite, "test_entities", "id", true)
	ctx := context.Background()

	t.Run("deve converter tipos corretamente ao ler do banco", func(t *testing.T) {
		db.Exec("DELETE FROM test_entities")

		now := time.Now()
		doc := &TestSQLEntity{
			Name:      "Teste Tipos",
			Age:       30,
			Active:    true,
			Score:     95.5,
			CreatedAt: now,
			UpdatedAt: now,
		}

		saved, err := store.Save(ctx, doc)
		assert.NoError(t, err)

		found, err := store.FindById(ctx, saved.ID)
		assert.NoError(t, err)

		assert.IsType(t, 0, found.ID)
		assert.IsType(t, "", found.Name)
		assert.IsType(t, 0, found.Age)
		assert.IsType(t, false, found.Active)
		assert.IsType(t, 0.0, found.Score)
		assert.IsType(t, time.Time{}, found.CreatedAt)
		assert.IsType(t, time.Time{}, found.UpdatedAt)
	})
}
