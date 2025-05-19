package store

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/luma-sys/go-db-store/enum"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/luma-sys/go-db-store/page"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// SQLStore implementa a interface Store para bancos de dados SQL
type SQLStore[T any] struct {
	db         *sql.DB
	tableName  string
	primaryKey string
}

// NewSQLStore cria uma nova instância de SQLStore
func NewSQLStore[T any](db *sql.DB, tableName string, primaryKey string) Store[T] {
	return &SQLStore[T]{
		db:         db,
		tableName:  tableName,
		primaryKey: primaryKey,
	}
}

// WithTransaction para SQL usa uma simples transação
func (s *SQLStore[T]) WithTransaction(ctx context.Context, fn TransactionDecorator) (any, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()

	// Call the function with tx (which implements TransactionContext)
	result, err := fn(tx)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return nil, fmt.Errorf("transaction error: %w, rollback error: %v", err, rollbackErr)
		}
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("erro ao fazer commit: %w", err)
	}

	return result, nil
}

// Has verifica se um registro existe pelo ID
func (s *SQLStore[T]) Has(ctx context.Context, id any) bool {
	query := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE %s = ?)", s.tableName, s.primaryKey)
	var exists bool
	err := s.db.QueryRowContext(ctx, query, id).Scan(&exists)
	return err == nil && exists
}

// Count retorna o número de registros baseado em uma consulta
func (s *SQLStore[T]) Count(ctx context.Context, q map[string]any) (*int64, error) {
	whereClause, values := s.buildWhereClause(q)
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", s.tableName)
	query += whereClause

	var count int64
	err := s.db.QueryRowContext(ctx, query, values...).Scan(&count)
	if err != nil {
		return nil, err
	}
	return &count, nil
}

// FindById busca um registro por ID
func (s *SQLStore[T]) FindById(ctx context.Context, id any) (*T, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", s.tableName, s.primaryKey)

	stmt, err := s.db.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("erro ao preparar query: %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.Query(id)
	if err != nil {
		return nil, fmt.Errorf("error querying room: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		return s.parseRow(rows)
	}

	return nil, fmt.Errorf("registro não encontrado")
}

// FindAll busca registros com paginação
func (s *SQLStore[T]) FindAll(ctx context.Context, f map[string]any, opts FindOptions) ([]T, error) {
	opts.Initialize()

	whereClause, values := s.buildWhereClause(f)
	query := fmt.Sprintf("SELECT * FROM %s", s.tableName)
	query += whereClause

	if opts.Limit > 0 {
		skip := page.Skip(opts.Page, opts.Limit)

		if s.isOracleDriver() {
			query = fmt.Sprintf("%s OFFSET :1 ROWS FETCH FIRST :2 ROWS ONLY", query)
			values = append(values, skip, opts.Limit)
		} else {
			query = fmt.Sprintf("%s LIMIT ? OFFSET ?", query)
			values = append(values, opts.Limit, skip)
		}
	}

	stmt, err := s.db.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("erro ao preparar query: %v", err)
	}
	defer stmt.Close()

	// Executa a query
	rows, err := stmt.Query(values...)
	if err != nil {
		return nil, fmt.Errorf("error querying %s: %w", s.tableName, err)
	}
	defer rows.Close()

	// Processa os resultados
	var results []T
	for rows.Next() {
		record, err := s.parseRow(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, *record)
	}

	return results, nil
}

// Save insere um novo registro
func (s *SQLStore[T]) Save(ctx context.Context, e *T) (*T, error) {
	// Implementação genérica requer reflexão
	v := reflect.ValueOf(e).Elem()
	fields := make([]string, 0)
	placeholders := make([]string, 0)
	values := make([]any, 0)

	for i := range v.NumField() {
		field := v.Type().Field(i)
		fieldName := field.Tag.Get("db")

		// Ignorar campos com tag `db:"-"`
		if fieldName == "-" {
			continue
		}

		if fieldName != s.primaryKey {
			fields = append(fields, fieldName)
			placeholders = append(placeholders, "?")
			values = append(values, v.Field(i).Interface())
		}
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		s.tableName,
		strings.Join(fields, ", "),
		strings.Join(placeholders, ", "),
	)

	result, err := s.db.ExecContext(ctx, query, values...)
	if err != nil {
		return nil, err
	}

	// Definir ID gerado se suportado
	if lastID, err := result.LastInsertId(); err == nil {
		// Atualizar o campo ID usando reflexão
		idField := v.FieldByName("ID")
		if idField.IsValid() && idField.CanSet() {
			idField.SetInt(lastID)
		}
	}

	return e, nil
}

// SaveMany insere múltiplos registros
func (s *SQLStore[T]) SaveMany(ctx context.Context, entities []T) (*InsertManyResult, error) {
	// Lógica similar a Save, mas em batch
	if len(entities) == 0 {
		return nil, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	for _, entity := range entities {
		// Chame o método Save para cada entidade
		_, err := s.Save(ctx, &entity)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	ids := make([]any, len(entities))
	for i, entity := range entities {
		v := reflect.ValueOf(&entity).Elem()
		idField := v.FieldByName("ID")
		if idField.IsValid() && idField.CanSet() {
			ids[i] = idField.Interface()
		}
	}
	return &InsertManyResult{InsertedIDs: ids}, nil
}

// Update atualiza um registro existente
func (s *SQLStore[T]) Update(ctx context.Context, e *T) (*T, error) {
	v := reflect.ValueOf(e).Elem()

	// Verifica se existe campo updated_at
	hasUpdatedAt := v.FieldByName("UpdatedAt").IsValid()

	// Preparar campos para atualização
	updates := make([]string, 0)
	values := make([]any, 0)
	var id any

	for i := range v.NumField() {
		field := v.Type().Field(i)
		fieldName := field.Tag.Get("db")

		if fieldName == s.primaryKey {
			id = v.Field(i).Interface()
		} else if field.Tag.Get("db") != "-" {
			updates = append(updates, fmt.Sprintf("%s = ?", fieldName))
			values = append(values, v.Field(i).Interface())
		}
	}

	// Se updated_at existe mas não foi definido pelo cliente, adiciona automaticamente
	if hasUpdatedAt {
		updates = append(updates, fmt.Sprintf("%s = ?", "updated_at"))
		values = append(values, time.Now())

		// Atualiza o valor no struct também
		for i := range v.NumField() {
			field := v.Type().Field(i)
			if field.Tag.Get("db") == "updated_at" {
				v.Field(i).Set(reflect.ValueOf(time.Now()))
				break
			}
		}
	}

	// Adicionar ID ao final dos valores
	values = append(values, id)

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s = ?",
		s.tableName,
		strings.Join(updates, ", "),
		s.primaryKey,
	)

	_, err := s.db.ExecContext(ctx, query, values...)
	if err != nil {
		return nil, err
	}

	return e, nil
}

// UpdateMany atualiza múltiplos registros
func (s *SQLStore[T]) UpdateMany(ctx context.Context, f map[string]any, updates map[string]any) (*UpdateResult, error) {
	// Verifica se o struct tem campo updated_at
	var dummy T
	t := reflect.TypeOf(dummy)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	hasUpdatedAt := false
	for i := range t.NumField() {
		field := t.Field(i)
		if field.Tag.Get("db") == "updated_at" {
			hasUpdatedAt = true
			break
		}
	}

	// Construir cláusula WHERE
	whereClause, _ := s.buildWhereClause(f)

	// Preparar campos de atualização
	setUpdates := make([]string, 0)
	values := make([]any, 0)

	for field, value := range updates {
		setUpdates = append(setUpdates, fmt.Sprintf("%s = ?", field))
		values = append(values, value)
	}

	// Se updated_at existe mas não foi definido pelo cliente, adiciona automaticamente
	if hasUpdatedAt {
		setUpdates = append(setUpdates, fmt.Sprintf("%s = ?", "updated_at"))
		values = append(values, time.Now())
	}

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE 1=1 %s",
		s.tableName,
		strings.Join(setUpdates, ", "),
		whereClause,
	)

	result, err := s.db.ExecContext(ctx, query, values...)
	if err != nil {
		return nil, err
	}

	rowsAffected, _ := result.RowsAffected()
	return &UpdateResult{MatchedCount: rowsAffected, ModifiedCount: rowsAffected}, nil
}

// Upsert cria ou atualiza um registro
func (s *SQLStore[T]) Upsert(ctx context.Context, e *T, f *StoreUpsertFilter) (*UpdateResult, error) {
	v := reflect.ValueOf(e).Elem()

	// Verifica se existe campo updated_at
	hasUpdatedAt := v.FieldByName("UpdatedAt").IsValid()

	// Preparar campos
	fields := make([]string, 0)
	placeholders := make([]string, 0)
	updates := make([]string, 0)
	values := make([]any, 0)

	upsertField := f.UpsertFieldKey
	if upsertField == "" {
		upsertField = s.primaryKey
	}

	for i := range v.NumField() {
		field := v.Type().Field(i)
		fieldName := field.Tag.Get("db")

		if field.Tag.Get("db") != "-" {
			fields = append(fields, fieldName)
			placeholders = append(placeholders, "?")
			values = append(values, v.Field(i).Interface())

			// Campos para atualização (exceto o campo de upsert)
			if fieldName != upsertField {
				updates = append(updates, fmt.Sprintf("%s = VALUES(%s)", fieldName, fieldName))
			}
		}
	}

	if hasUpdatedAt {
		updates = append(updates, fmt.Sprintf("%s = ?", "updated_at"))
		values = append(values, time.Now())
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		s.tableName,
		strings.Join(fields, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(updates, ", "),
	)

	result, err := s.db.ExecContext(ctx, query, values...)
	if err != nil {
		return nil, err
	}

	rowsAffected, _ := result.RowsAffected()
	return &UpdateResult{UpsertedCount: rowsAffected}, nil
}

// UpsertMany cria ou atualiza múltiplos registros
func (s *SQLStore[T]) UpsertMany(ctx context.Context, entities []T, f *StoreUpsertFilter) (*BulkWriteResult, error) {
	if len(entities) == 0 {
		return nil, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	for _, entity := range entities {
		_, err := s.Upsert(ctx, &entity, f)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return &BulkWriteResult{UpsertedCount: int64(len(entities))}, nil
}

// Delete remove um registro pelo ID
func (s *SQLStore[T]) Delete(ctx context.Context, id any) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", s.tableName, s.primaryKey)
	_, err := s.db.ExecContext(ctx, query, id)
	return err
}

// DeleteMany remove múltiplos registros
func (s *SQLStore[T]) DeleteMany(ctx context.Context, f map[string]any) (*DeleteResult, error) {
	whereClause, values := s.buildWhereClause(f)
	query := fmt.Sprintf("DELETE FROM %s", s.tableName)
	query += whereClause

	result, err := s.db.ExecContext(ctx, query, values)
	if err != nil {
		return nil, err
	}

	rowsAffected, _ := result.RowsAffected()
	return &DeleteResult{DeletedCount: rowsAffected}, nil
}

func (s *SQLStore[T]) isOracleDriver() bool {
	// Para Oracle
	var version string
	err := s.db.QueryRow("SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'").Scan(&version)
	if err == nil && strings.Contains(strings.ToLower(version), "oracle") {
		return true
	}

	return false
}

// buildWhereClause constrói a cláusula WHERE baseada nos filtros fornecidos.
//
// Operadores suportados:
//
//	Like:
//		var filter = map[string]any{"name__like": "%John%"}
//		// Gera: name LIKE '%John%'
//
//	ILike (case insensitive):
//		var filter = map[string]any{"name__ilike": "%john%"}
//		// Gera: name ILIKE '%john%'
//
//	Not Like:
//		var filter = map[string]any{"name__not_like": "%John%"}
//		// Gera: name NOT LIKE '%John%'
//
//	Equal:
//		var filter = map[string]any{"name": "John"}
//		// Gera: name = ?
//
//	Not Equal:
//		var filter = map[string]any{"name__not": "John"}
//		// Gera: name != ?
//
//	In:
//		var filter = map[string]any{"name__in": []string{"John", "Jane"}}
//		// Gera: name IN (?, ?)
//
//	Is Null:
//		var filter = map[string]any{"name__is_null": true}
//		// Gera: name IS NULL
//
//	Is Not Null:
//		var filter = map[string]any{"name__is_not_null": true}
//		// Gera: name IS NOT NULL
//
//	Comparações numéricas:
//		var filter = map[string]any{
//			"age__gt": 30,      // age > 30
//			"age__lt": 60,      // age < 60
//			"age__gte": 18,     // age >= 18
//			"age__lte": 65,     // age <= 65
//		}
func (s *SQLStore[T]) buildWhereClause(filters map[string]any) (string, []any) {
	if len(filters) == 0 {
		return "", make([]any, 0)
	}

	// Ordena as chaves
	keys := make([]string, 0, len(filters))
	for key := range filters {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	whereConditions := make([]string, 0, len(keys))
	values := make([]any, 0, len(keys))

	for _, key := range keys {
		operator := "="
		field := key
		value := filters[key]

		if strings.Contains(key, "__") {
			parts := strings.Split(key, "__")
			field = parts[0]
			switch parts[1] {
			case "like":
				operator = "LIKE"
			case "ilike":
				operator = "ILIKE"
			case "not_like":
				operator = "NOT LIKE"
			case "gt":
				operator = ">"
			case "lt":
				operator = "<"
			case "gte":
				operator = ">="
			case "lte":
				operator = "<="
			case "in":
				operator = "IN"
			case "not":
				operator = "!="
			case "is_null":
				operator = "IS NULL"
			case "is_not_null":
				operator = "IS NOT NULL"
			}
		}

		if operator == "IS NULL" || operator == "IS NOT NULL" {
			whereConditions = append(whereConditions, fmt.Sprintf("%s %s", field, operator))
			continue
		}

		whereConditions = append(whereConditions, fmt.Sprintf("%s %s ?", field, operator))
		values = append(values, value)
	}

	return " WHERE " + strings.Join(whereConditions, " AND "), values
}

// toCamelCase Converte snake_case para CamelCase
func (s *SQLStore[T]) toCamelCase(value string) string {
	capitalize := cases.Title(language.Portuguese)
	parts := strings.Split(value, "_")
	for i := range parts {
		if parts[i] == "id" {
			parts[i] = "ID"
			continue
		}

		parts[i] = capitalize.String(parts[i])
	}
	return strings.Join(parts, "")
}

// setValue Função auxiliar para definir valores com conversão de tipo
func (s *SQLStore[T]) setValue(field reflect.Value, value any) {
	if !field.CanSet() {
		return
	}

	switch field.Kind() {
	case reflect.Ptr:
		// Para tipos ponteiro, crie um novo ponteiro se o valor não for nulo
		if value != nil {
			// Obtém o tipo do elemento do ponteiro
			elemType := field.Type().Elem()

			// Cria uma nova instância do tipo do enum
			newInstance := reflect.New(elemType).Interface()

			// Tenta converter usando a interface StringConverter através de uma type assertion
			if converter, ok := newInstance.(enum.StringConverter); ok {
				var strValue string
				switch v := value.(type) {
				case []byte:
					strValue = string(v)
				default:
					strValue = fmt.Sprintf("%v", value)
				}

				// Converte a string para o tipo específico
				converted, err := converter.FromString(strValue)
				if err == nil {
					// Define o valor no campo
					field.Set(reflect.ValueOf(converted))
				} else {
					fmt.Printf("Erro ao converter para %s: %v\n", elemType.String(), err)
				}
				return
			}

			// Cria um novo valor do tipo correto
			newValue := reflect.New(elemType)

			// Converte o val	or para o tipo correto
			convertedValue, err := s.convertToType(reflect.ValueOf(value), elemType)
			if err != nil {
				// Lida com erro de conversão
				fmt.Printf("Erro ao converter valor: %v\n", err)
				return
			}

			// Define o valor no elemento do ponteiro
			newValue.Elem().Set(convertedValue)

			// Define o ponteiro
			field.Set(newValue)
		}
	case reflect.String:
		if v, ok := value.([]byte); ok {
			field.SetString(string(v))
		} else {
			field.SetString(fmt.Sprintf("%v", value))
		}
	case reflect.Int, reflect.Int64:
		switch v := value.(type) {
		case int:
			field.SetInt(int64(v))
		case int64:
			field.SetInt(v)
		case []byte:
			intVal, _ := strconv.ParseInt(string(v), 10, 64)
			field.SetInt(intVal)
		}
	case reflect.Float64:
		switch v := value.(type) {
		case float64:
			field.SetFloat(v)
		case []byte:
			floatVal, _ := strconv.ParseFloat(string(v), 64)
			field.SetFloat(floatVal)
		}
	case reflect.Struct:
		// Para tipos Time, conversão específica
		if field.Type().String() == "time.Time" {
			if v, ok := value.(time.Time); ok {
				field.Set(reflect.ValueOf(v))
			} else if v, ok := value.([]byte); ok {
				t, _ := time.Parse("2006-01-02 15:04:05", string(v))
				field.Set(reflect.ValueOf(t))
			}
		}
	}
}

// convertToType Função auxiliar de conversão de tipo
func (s *SQLStore[T]) convertToType(value reflect.Value, targetType reflect.Type) (reflect.Value, error) {
	// Se o valor já é do tipo correto, retorna
	if value.Type() == targetType {
		return value, nil
	}

	// Trata conversões para tipos básicos
	switch targetType.Kind() {
	case reflect.String:
		return reflect.ValueOf(fmt.Sprintf("%v", value.Interface())), nil

	case reflect.Int, reflect.Int64:
		switch v := value.Interface().(type) {
		case []uint8:
			intVal, err := strconv.ParseInt(string(v), 10, 64)
			if err != nil {
				return reflect.Value{}, err
			}
			return reflect.ValueOf(intVal), nil
		case int, int64, uint, uint64:
			return reflect.ValueOf(value.Convert(targetType).Interface()), nil
		default:
			intVal, err := strconv.ParseInt(fmt.Sprintf("%v", v), 10, 64)
			if err != nil {
				return reflect.Value{}, err
			}
			return reflect.ValueOf(intVal), nil
		}

	case reflect.Float64:
		switch v := value.Interface().(type) {
		case []uint8:
			floatVal, err := strconv.ParseFloat(string(v), 64)
			if err != nil {
				return reflect.Value{}, err
			}
			return reflect.ValueOf(floatVal), nil
		case float64, float32:
			return reflect.ValueOf(value.Convert(targetType).Interface()), nil
		default:
			floatVal, err := strconv.ParseFloat(fmt.Sprintf("%v", v), 64)
			if err != nil {
				return reflect.Value{}, err
			}
			return reflect.ValueOf(floatVal), nil
		}

	case reflect.Struct:
		// Tratamento especial para time.Time
		if targetType.String() == "time.Time" {
			switch v := value.Interface().(type) {
			case time.Time:
				return reflect.ValueOf(v), nil
			case []uint8:
				t, err := time.Parse("2006-01-02 15:04:05", string(v))
				if err != nil {
					return reflect.Value{}, err
				}
				return reflect.ValueOf(t), nil
			}
		}
	}

	// Se nenhuma conversão específica funcionar, tenta conversão genérica
	return reflect.ValueOf(value.Convert(targetType).Interface()), nil
}

// parseRow Função auxiliar de parse de linha do banco
func (s *SQLStore[T]) parseRow(rows *sql.Rows) (*T, error) {
	// Obtém os nomes das colunas
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("erro ao obter colunas: %v", err)
	}

	// Cria um slice de valores para scan
	values := make([]any, len(columns))
	valuePtrs := make([]any, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	// Faz o scan
	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	// Cria a estrutura de retorno
	entity := reflect.New(reflect.TypeOf((*T)(nil)).Elem()).Interface().(*T)
	v := reflect.ValueOf(entity).Elem()
	t := v.Type()

	// Criar um mapa de tags 'db' para campos
	dbTagToField := make(map[string]reflect.Value)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		typeField := t.Field(i)
		tag := typeField.Tag.Get("db")
		if tag != "" && tag != "-" {
			dbTagToField[tag] = field
		}
	}

	// Mapeia os valores para os campos usando as tags 'db'
	for i, column := range columns {
		// Procura pelo campo com a tag 'db' correspondente
		if field, ok := dbTagToField[column]; ok && field.IsValid() {
			// Converte e atribui o valor
			s.setValue(field, values[i])
		}
	}

	return entity, nil
}
