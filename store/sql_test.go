package store

import (
	"context"
	"database/sql"
	"errors"
	"github.com/luma-sys/go-db-store/enum"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type YourEntityType struct {
	ID        int       `db:"id" json:"id"`
	Name      string    `db:"name" json:"name"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
}

func setupDB() (*sql.DB, error) {
	// Setup
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, errors.New("erro ao abrir conexão com SQLite: " + err.Error())
	}

	// Criar tabelas para o teste
	_, err = db.Exec(`
		CREATE TABLE your_table_name (
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		return nil, errors.New("erro ao criar tabela: " + err.Error())
	}

	return db, nil
}

// TestHas verifica se um registro existe pelo ID
func TestHas(t *testing.T) {
	// Setup
	db, err := setupDB()
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}
	store := NewSQLStore[YourEntityType](db, enum.DatabaseDriverSqlite, "your_table_name", "id", true)

	// Teste de inserção
	_, _ = store.Save(context.Background(), &YourEntityType{ID: 1, Name: "John"})

	// Teste se o registro existe
	exists := store.Has(context.Background(), 1)
	assert.True(t, exists)

	// Teste se um ID inexistente retorna false
	exists = store.Has(context.Background(), 2)
	assert.False(t, exists)
}

// TestCount retorna o número de registros baseado em uma consulta
func TestCount(t *testing.T) {
	// Setup
	db, err := setupDB()
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}
	store := NewSQLStore[YourEntityType](db, enum.DatabaseDriverSqlite, "your_table_name", "id", true)
	_, _ = store.Save(context.Background(), &YourEntityType{ID: 1, Name: "John"})
	_, _ = store.Save(context.Background(), &YourEntityType{ID: 2, Name: "Jane"})

	// Teste de contagem
	count, err := store.Count(context.Background(), map[string]any{})
	assert.NoError(t, err)
	assert.Equal(t, int64(2), *count)
}

func TestFindById(t *testing.T) {
	// Setup
	db, err := setupDB()
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}
	store := NewSQLStore[YourEntityType](db, enum.DatabaseDriverSqlite, "your_table_name", "id", true)

	now := time.Now()

	tests := []struct {
		name    string
		setup   func() *YourEntityType
		id      any
		want    func(*testing.T, *YourEntityType)
		wantErr bool
	}{
		{
			name: "deve encontrar registro existente",
			setup: func() *YourEntityType {
				entity := &YourEntityType{
					ID:        1,
					Name:      "Registro Teste",
					CreatedAt: now,
					UpdatedAt: now,
				}
				_, err := store.Save(context.Background(), entity)
				assert.NoError(t, err)
				return entity
			},
			id: 1,
			want: func(t *testing.T, got *YourEntityType) {
				assert.Equal(t, 1, got.ID)
				assert.Equal(t, "Registro Teste", got.Name)
				assert.Equal(t, now.UTC().Format("2006-01-02 15:04:05"),
					got.CreatedAt.UTC().Format("2006-01-02 15:04:05"))
			},
			wantErr: false,
		},
		{
			name: "deve retornar erro para registro inexistente",
			setup: func() *YourEntityType {
				return nil
			},
			id:      999,
			want:    nil,
			wantErr: true,
		},
		{
			name: "deve encontrar registro com campos nulos",
			setup: func() *YourEntityType {
				entity := &YourEntityType{
					ID:   2,
					Name: "",
				}
				_, err := store.Save(context.Background(), entity)
				assert.NoError(t, err)
				return entity
			},
			id: 2,
			want: func(t *testing.T, got *YourEntityType) {
				assert.Equal(t, 2, got.ID)
				assert.Equal(t, "", got.Name)
			},
			wantErr: false,
		},
		{
			name: "deve manter tipos de dados corretos",
			setup: func() *YourEntityType {
				entity := &YourEntityType{
					ID:        3,
					Name:      "Test Types",
					CreatedAt: now,
					UpdatedAt: now,
				}
				_, err := store.Save(context.Background(), entity)
				assert.NoError(t, err)
				return entity
			},
			id: 3,
			want: func(t *testing.T, got *YourEntityType) {
				assert.IsType(t, 0, got.ID)
				assert.IsType(t, "", got.Name)
				assert.IsType(t, time.Time{}, got.CreatedAt)
				assert.IsType(t, time.Time{}, got.UpdatedAt)
			},
			wantErr: false,
		},
		{
			name: "deve retornar erro para ID inválido",
			setup: func() *YourEntityType {
				return nil
			},
			id:      "invalid_id",
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Executa setup
			expected := tt.setup()

			// Executa FindById
			got, err := store.FindById(context.Background(), tt.id)

			// Verifica erro
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, got)
				return
			}

			// Verifica sucesso
			assert.NoError(t, err)
			assert.NotNil(t, got)

			// Executa verificações específicas
			if tt.want != nil {
				tt.want(t, got)
			}

			// Se tiver registro esperado, compara com o obtido
			if expected != nil {
				assert.Equal(t, expected.ID, got.ID)
				assert.Equal(t, expected.Name, got.Name)
				assert.Equal(t, expected.CreatedAt.UTC().Format("2006-01-02 15:04:05"),
					got.CreatedAt.UTC().Format("2006-01-02 15:04:05"))
				assert.Equal(t, expected.UpdatedAt.UTC().Format("2006-01-02 15:04:05"),
					got.UpdatedAt.UTC().Format("2006-01-02 15:04:05"))
			}
		})
	}
}

func TestSave(t *testing.T) {
	// Setup
	db, err := setupDB()
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}
	store := NewSQLStore[YourEntityType](db, enum.DatabaseDriverSqlite, "your_table_name", "id", true)

	now := time.Now()

	tests := []struct {
		name    string
		input   *YourEntityType
		check   func(*testing.T, *YourEntityType)
		wantErr bool
	}{
		{
			name: "deve salvar registro com todos os campos",
			input: &YourEntityType{
				ID:        2,
				Name:      "Teste Completo",
				CreatedAt: now,
				UpdatedAt: now,
			},
			check: func(t *testing.T, result *YourEntityType) {
				assert.Equal(t, 2, result.ID)
				assert.Equal(t, "Teste Completo", result.Name)
				assert.NotZero(t, result.CreatedAt)
				assert.NotZero(t, result.UpdatedAt)

				// Verifica se o registro foi realmente salvo no banco
				saved, err := store.FindById(context.Background(), result.ID)
				assert.NoError(t, err)
				assert.Equal(t, result.ID, saved.ID)
				assert.Equal(t, result.Name, saved.Name)
			},
		},
		{
			name: "deve salvar registro com campos mínimos",
			input: &YourEntityType{
				Name: "Campos Mínimos",
			},
			check: func(t *testing.T, result *YourEntityType) {
				assert.NotNil(t, result.ID)
				assert.Equal(t, "Campos Mínimos", result.Name)
				assert.NotNil(t, result.CreatedAt)
				assert.NotNil(t, result.UpdatedAt)
			},
		},
		{
			name: "deve gerar ID automático",
			input: &YourEntityType{
				Name: "ID Automático",
			},
			check: func(t *testing.T, result *YourEntityType) {
				assert.NotNil(t, result.ID)
				count, err := store.Count(context.Background(), map[string]any{"id": result.ID})
				assert.NoError(t, err)
				assert.Equal(t, int64(1), *count)
			},
		},
		{
			name: "deve salvar registro com string vazia",
			input: &YourEntityType{
				Name: "",
			},
			check: func(t *testing.T, result *YourEntityType) {
				assert.NotZero(t, result.ID)
				assert.Empty(t, result.Name)
			},
		},
		{
			name: "deve manter tipos de dados corretos",
			input: &YourEntityType{
				ID:        123,
				Name:      "Teste Tipos",
				CreatedAt: now,
				UpdatedAt: now,
			},
			check: func(t *testing.T, result *YourEntityType) {
				assert.IsType(t, 0, result.ID)
				assert.IsType(t, "", result.Name)
				assert.IsType(t, time.Time{}, result.CreatedAt)
				assert.IsType(t, time.Time{}, result.UpdatedAt)
			},
		},
		// Para esse cenário, o bando não deve ser auto incrementado
		//{
		//	name: "deve verificar unicidade de ID",
		//	input: &YourEntityType{
		//		ID:   1, // ID já utilizado no primeiro teste
		//		Name: "Teste Duplicado",
		//	},
		//	wantErr: true,
		//},
		{
			name: "deve lidar com timestamps diferentes",
			input: &YourEntityType{
				Name:      "Teste Timestamps",
				CreatedAt: now.Add(-24 * time.Hour),
				UpdatedAt: now.Add(-12 * time.Hour),
			},
			check: func(t *testing.T, result *YourEntityType) {
				assert.NotEqual(t, result.CreatedAt, result.UpdatedAt)
				assert.True(t, result.UpdatedAt.After(result.CreatedAt))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Executa Save
			_, _ = store.Save(context.Background(), &YourEntityType{ID: 1, Name: "John"})
			result, err := store.Save(context.Background(), tt.input)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			// Verifica sucesso
			assert.NoError(t, err)
			assert.NotNil(t, result)

			// Executa verificações específicas
			if tt.check != nil {
				tt.check(t, result)
			}

			// Verifica se o registro existe no banco
			if !tt.wantErr {
				exists := store.Has(context.Background(), result.ID)
				assert.True(t, exists)
			}
		})
	}
}

func TestFindAll(t *testing.T) {
	// Setup
	db, err := setupDB()
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}
	store := NewSQLStore[YourEntityType](db, enum.DatabaseDriverSqlite, "your_table_name", "id", true)

	// Preparar dados de teste
	testData := []YourEntityType{
		{ID: 1, Name: "João"},
		{ID: 2, Name: "Maria"},
		{ID: 3, Name: "Pedro"},
		{ID: 4, Name: "Ana"},
	}

	// Inserir dados de teste
	for _, entity := range testData {
		_, err := store.Save(context.Background(), &entity)
		assert.NoError(t, err)
	}

	tests := []struct {
		name    string
		filter  map[string]any
		opts    FindOptions
		wantLen int
		wantErr bool
		checkFn func(t *testing.T, results []YourEntityType)
	}{
		{
			name:    "deve retornar todos os registros sem filtro",
			filter:  map[string]any{},
			opts:    FindOptions{},
			wantLen: 4,
			checkFn: func(t *testing.T, results []YourEntityType) {
				assert.Equal(t, 4, len(results))
				assert.Equal(t, "João", results[0].Name)
			},
		},
		{
			name: "deve retornar registros filtrados por nome",
			filter: map[string]any{
				"name": "João",
			},
			opts:    FindOptions{},
			wantLen: 1,
			checkFn: func(t *testing.T, results []YourEntityType) {
				assert.Equal(t, 1, len(results))
				assert.Equal(t, "João", results[0].Name)
			},
		},
		{
			name:   "deve retornar registros com paginação",
			filter: map[string]any{},
			opts: FindOptions{
				Page:  1,
				Limit: 2,
			},
			wantLen: 2,
			checkFn: func(t *testing.T, results []YourEntityType) {
				assert.Equal(t, 2, len(results))
			},
		},
		{
			name: "deve retornar registros com like",
			filter: map[string]any{
				"name__like": "%a%",
			},
			opts:    FindOptions{},
			wantLen: 2, // Maria e Ana
			checkFn: func(t *testing.T, results []YourEntityType) {
				assert.Equal(t, 2, len(results))
				for _, r := range results {
					assert.Contains(t, r.Name, "a")
				}
			},
		},
		{
			name: "deve retornar registros com in",
			filter: map[string]any{
				"id__in": []int{1, 2},
			},
			opts:    FindOptions{},
			wantLen: 2,
			checkFn: func(t *testing.T, results []YourEntityType) {
				assert.Equal(t, 2, len(results))
				ids := []int{results[0].ID, results[1].ID}
				assert.Contains(t, ids, 1)
				assert.Contains(t, ids, 2)
			},
		},
		{
			name: "deve retornar vazio quando não encontrar registros",
			filter: map[string]any{
				"name": "NãoExiste",
			},
			opts:    FindOptions{},
			wantLen: 0,
			checkFn: func(t *testing.T, results []YourEntityType) {
				assert.Equal(t, 0, len(results))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := store.FindAll(context.Background(), tt.filter, tt.opts)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.wantLen, len(results))

			if tt.checkFn != nil {
				tt.checkFn(t, results)
			}
		})
	}
}

func TestUpdate(t *testing.T) {
	// Setup
	db, err := setupDB()
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}
	store := NewSQLStore[YourEntityType](db, enum.DatabaseDriverSqlite, "your_table_name", "id", true)

	now := time.Now().UTC()

	tests := []struct {
		name    string
		setup   func() *YourEntityType
		update  func(*YourEntityType) error
		check   func(*testing.T, *YourEntityType)
		wantErr bool
	}{
		{
			name: "deve atualizar registro com sucesso",
			setup: func() *YourEntityType {
				entity := &YourEntityType{ID: 1, Name: "Nome Original"}
				_, err := store.Save(context.Background(), entity)
				assert.NoError(t, err)
				return entity
			},
			update: func(e *YourEntityType) error {
				e.Name = "Nome Atualizado"
				_, err := store.Update(context.Background(), e)
				return err
			},
			check: func(t *testing.T, e *YourEntityType) {
				updated, err := store.FindById(context.Background(), e.ID)
				time.Sleep(1 * time.Second)
				assert.NoError(t, err)
				assert.Equal(t, "Nome Atualizado", updated.Name)
			},
		},
		{
			name: "deve atualizar timestamp updated_at",
			setup: func() *YourEntityType {
				entity := &YourEntityType{
					ID:        2,
					Name:      "Nome Original",
					CreatedAt: now,
					UpdatedAt: now,
				}
				_, err := store.Save(context.Background(), entity)
				assert.NoError(t, err)
				return entity
			},
			update: func(e *YourEntityType) error {
				e.Name = "Nome Atualizado"
				_, err := store.Update(context.Background(), e)
				return err
			},
			check: func(t *testing.T, e *YourEntityType) {
				updated, err := store.FindById(context.Background(), e.ID)
				time.Sleep(1 * time.Second)
				assert.NoError(t, err)
				assert.True(t, updated.UpdatedAt.After(now))
			},
		},
		{
			name: "deve falhar ao atualizar registro inexistente",
			setup: func() *YourEntityType {
				return &YourEntityType{ID: 9999, Name: "Não Existe"}
			},
			update: func(e *YourEntityType) error {
				_, err := store.Update(context.Background(), e)
				return err
			},
			wantErr: true,
		},
		{
			name: "deve atualizar múltiplos campos",
			setup: func() *YourEntityType {
				entity := &YourEntityType{
					ID:        4,
					Name:      "Nome Original",
					CreatedAt: now,
					UpdatedAt: now,
				}
				_, err := store.Save(context.Background(), entity)
				assert.NoError(t, err)
				return entity
			},
			update: func(e *YourEntityType) error {
				e.Name = "Nome Atualizado"
				_, err := store.Update(context.Background(), e)
				return err
			},
			check: func(t *testing.T, e *YourEntityType) {
				updated, err := store.FindById(context.Background(), e.ID)
				assert.NoError(t, err)
				assert.Equal(t, "Nome Atualizado", updated.Name)
				assert.NotNil(t, updated.UpdatedAt)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Executa setup
			entity := tt.setup()

			// Executa update
			err := tt.update(entity)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			// Executa verificações
			if tt.check != nil {
				tt.check(t, entity)
			}
		})
	}
}

func TestUpsert(t *testing.T) {
	// Setup
	db, err := setupDB()
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}
	store := NewSQLStore[YourEntityType](db, enum.DatabaseDriverSqlite, "your_table_name", "id", true)

	now := time.Now().UTC()

	tests := []struct {
		name    string
		setup   func() *YourEntityType
		execute func(*YourEntityType) (*UpdateResult, error)
		check   func(*testing.T, *YourEntityType)
		wantErr bool
	}{
		{
			name: "deve inserir novo registro quando não existe",
			setup: func() *YourEntityType {
				return &YourEntityType{
					ID:   1,
					Name: "Registro Novo",
				}
			},
			execute: func(e *YourEntityType) (*UpdateResult, error) {
				return store.Upsert(context.Background(), e, &StoreUpsertFilter{})
			},
			check: func(t *testing.T, e *YourEntityType) {
				// Verifica se o registro foi inserido
				exists := store.Has(context.Background(), e.ID)
				assert.True(t, exists)

				// Verifica os dados inseridos
				record, err := store.FindById(context.Background(), e.ID)
				assert.NoError(t, err)
				assert.Equal(t, e.Name, record.Name)
			},
		},
		{
			name: "deve atualizar registro existente",
			setup: func() *YourEntityType {
				entity := &YourEntityType{
					ID:   2,
					Name: "Nome Original",
				}
				_, _ = store.Save(context.Background(), entity)
				entity.Name = "Nome Atualizado"
				return entity
			},
			execute: func(e *YourEntityType) (*UpdateResult, error) {
				return store.Upsert(context.Background(), e, &StoreUpsertFilter{})
			},
			check: func(t *testing.T, e *YourEntityType) {
				record, err := store.FindById(context.Background(), e.ID)
				assert.NoError(t, err)
				assert.Equal(t, "Nome Atualizado", record.Name)
			},
		},
		{
			name: "deve atualizar campo updated_at",
			setup: func() *YourEntityType {
				entity := &YourEntityType{
					ID:        3,
					Name:      "Nome Original",
					UpdatedAt: now,
				}
				_, _ = store.Save(context.Background(), entity)
				entity.Name = "Nome Atualizado"
				return entity
			},
			execute: func(e *YourEntityType) (*UpdateResult, error) {
				return store.Upsert(context.Background(), e, &StoreUpsertFilter{})
			},
			check: func(t *testing.T, e *YourEntityType) {
				record, err := store.FindById(context.Background(), e.ID)
				assert.NoError(t, err)
				assert.True(t, record.UpdatedAt.Equal(e.UpdatedAt))
			},
		},
		{
			name: "deve usar campo personalizado para upsert",
			setup: func() *YourEntityType {
				return &YourEntityType{
					ID:   4,
					Name: "Nome Único",
				}
			},
			execute: func(e *YourEntityType) (*UpdateResult, error) {
				return store.Upsert(context.Background(), e, &StoreUpsertFilter{
					UpsertFieldKey: "name",
				})
			},
			check: func(t *testing.T, e *YourEntityType) {
				// Verifica se o registro foi inserido usando o campo personalizado
				results, err := store.FindAll(context.Background(), map[string]any{"name": e.Name}, FindOptions{})
				assert.NoError(t, err)
				assert.Equal(t, 1, len(results))
				assert.Equal(t, e.Name, results[0].Name)
			},
		},
		{
			name: "deve preservar campos não atualizáveis",
			setup: func() *YourEntityType {
				entity := &YourEntityType{
					ID:        5,
					Name:      "Nome Original",
					CreatedAt: now,
				}
				_, _ = store.Save(context.Background(), entity)
				entity.Name = "Nome Atualizado"
				entity.CreatedAt = now.Add(1 * time.Hour)
				return entity
			},
			execute: func(e *YourEntityType) (*UpdateResult, error) {
				return store.Upsert(context.Background(), e, &StoreUpsertFilter{})
			},
			check: func(t *testing.T, e *YourEntityType) {
				record, err := store.FindById(context.Background(), e.ID)
				assert.NoError(t, err)
				assert.Equal(t, "Nome Atualizado", record.Name)
				assert.Equal(t, e.CreatedAt, record.CreatedAt)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepara o teste
			entity := tt.setup()

			// Executa o upsert
			result, err := tt.execute(entity)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Greater(t, result.UpsertedCount, int64(0))

			// Executa verificações específicas
			if tt.check != nil {
				tt.check(t, entity)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	// Setup
	db, err := setupDB()
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}
	store := NewSQLStore[YourEntityType](db, enum.DatabaseDriverSqlite, "your_table_name", "id", true)

	tests := []struct {
		name    string
		setup   func() (int, error)
		wantErr bool
	}{
		{
			name: "deve deletar registro existente com sucesso",
			setup: func() (int, error) {
				entity := &YourEntityType{
					ID:   1,
					Name: "Registro para Deletar",
				}
				_, err := store.Save(context.Background(), entity)
				return entity.ID, err
			},
			wantErr: false,
		},
		{
			name: "deve retornar sucesso ao tentar deletar registro inexistente",
			setup: func() (int, error) {
				return 999, nil
			},
			wantErr: false,
		},
		{
			name: "deve validar integridade após deleção",
			setup: func() (int, error) {
				// Salva dois registros
				entity1 := &YourEntityType{ID: 1, Name: "Registro 1"}
				entity2 := &YourEntityType{ID: 2, Name: "Registro 2"}

				_, err := store.Save(context.Background(), entity1)
				if err != nil {
					return 0, err
				}
				_, err = store.Save(context.Background(), entity2)
				return entity1.ID, err
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Executa setup e obtém ID para deleção
			id, err := tt.setup()
			assert.NoError(t, err)

			// Executa a deleção
			err = store.Delete(context.Background(), id)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			// Verifica se o registro foi realmente deletado
			exists := store.Has(context.Background(), id)
			assert.False(t, exists, "Registro ainda existe após deleção")

			// Caso especial para teste de integridade
			if tt.name == "deve validar integridade após deleção" {
				// Verifica se o outro registro ainda existe
				exists = store.Has(context.Background(), 3)
				assert.True(t, exists, "Registro que não deveria ser afetado foi deletado")

				// Verifica contagem total
				count, err := store.Count(context.Background(), map[string]any{})
				assert.NoError(t, err)
				assert.Equal(t, int64(1), *count, "Número incorreto de registros após deleção")
			}
		})
	}
}
