package store

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/luma-sys/go-db-store/enum"
	"github.com/luma-sys/go-db-store/page"
)

type SQLStore[T any] struct {
	db            *sql.DB
	driver        enum.DatabaseDriver
	tableName     string
	primaryKey    string
	autoincrement bool
}

func NewSQLStore[T any](db *sql.DB, driver enum.DatabaseDriver, tableName string, primaryKey string, autoincrement bool) Store[T] {
	return &SQLStore[T]{
		db:            db,
		driver:        driver,
		tableName:     tableName,
		primaryKey:    primaryKey,
		autoincrement: autoincrement,
	}
}

// WithTransaction para SQL usa uma simples transação
func (s *SQLStore[T]) WithTransaction(ctx context.Context, fn Transaction) (any, error) {
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

	result, err := fn(tx)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return nil, fmt.Errorf("transaction error: %w, rollback error: %v", err, rollbackErr)
		}
		return nil, fmt.Errorf("transaction error: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("erro ao fazer commit: %w", err)
	}

	return result, nil
}

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

func (s *SQLStore[T]) FindOne(ctx context.Context, f map[string]interface{}) (*T, error) {
	whereClause, values := s.buildWhereClause(f)
	query := fmt.Sprintf("SELECT * FROM %s", s.tableName)
	query += whereClause

	// Oracle não suporta LIMIT, usa FETCH FIRST
	if s.driver == enum.DatabaseDriverOracle {
		query += " FETCH FIRST 1 ROWS ONLY"
	} else {
		query += " LIMIT 1"
	}

	stmt, err := s.db.Prepare(query)
	if err != nil {
		return nil, fmt.Errorf("erro ao preparar query: %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx, values...)
	if err != nil {
		return nil, fmt.Errorf("erro ao buscar documento: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		result, err := s.parseRow(rows)
		if err != nil {
			return nil, fmt.Errorf("erro ao decodificar documento: %w", err)
		}
		return result, nil
	}

	return nil, fmt.Errorf("documento não encontrado com filtro %v", f)
}

// FindAll busca registros com paginação
func (s *SQLStore[T]) FindAll(ctx context.Context, f map[string]any, opts FindOptions) ([]T, error) {
	opts.Initialize()

	whereClause, values := s.buildWhereClause(f)
	query := fmt.Sprintf("SELECT * FROM %s", s.tableName)
	query += whereClause

	if opts.Limit > 0 {
		skip := page.Skip(opts.Page, opts.Limit)

		if s.driver == enum.DatabaseDriverOracle {
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

		if fieldName == s.primaryKey && s.autoincrement {
			continue
		}

		fields = append(fields, fieldName)
		placeholders = append(placeholders, "?")
		values = append(values, v.Field(i).Interface())
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

	// Definir ID gerado se suportado (Oracle não suporta LastInsertId)
	if lastID, err := result.LastInsertId(); err == nil && lastID > 0 {
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
	if len(entities) == 0 {
		return nil, nil
	}

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

	ids := make([]any, len(entities))

	for i, entity := range entities {
		v := reflect.ValueOf(&entity).Elem()
		fields := make([]string, 0)
		placeholders := make([]string, 0)
		values := make([]any, 0)

		for j := range v.NumField() {
			field := v.Type().Field(j)
			fieldName := field.Tag.Get("db")

			if fieldName == "-" {
				continue
			}

			if fieldName == s.primaryKey && s.autoincrement {
				continue
			}

			fields = append(fields, fieldName)
			placeholders = append(placeholders, "?")
			values = append(values, v.Field(j).Interface())
		}

		query := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			s.tableName,
			strings.Join(fields, ", "),
			strings.Join(placeholders, ", "),
		)

		// Usa tx.ExecContext em vez de s.db.ExecContext
		result, err := tx.ExecContext(ctx, query, values...)
		if err != nil {
			tx.Rollback()
			return nil, err
		}

		if lastID, err := result.LastInsertId(); err == nil {
			ids[i] = lastID
			idField := v.FieldByName("ID")
			if idField.IsValid() && idField.CanSet() {
				idField.SetInt(lastID)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &InsertManyResult{InsertedIDs: ids}, nil
}

// SaveManyNotOrdered [NOT IMPLEMENTED] salva vários registros de forma desordenada
func (s *SQLStore[T]) SaveManyNotOrdered(ctx context.Context, e []T) (*InsertManyResult, error) {
	return nil, fmt.Errorf("not implemented by SQL module")
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

	result, err := s.db.ExecContext(ctx, query, values...)
	if err != nil {
		return nil, err
	}

	if rowsAffected, err := result.RowsAffected(); err == nil {
		if rowsAffected == 0 {
			return nil, fmt.Errorf("registro não encontrado")
		}
	}

	return e, nil
}

// UpdateMany atualiza atributos de múltiplos registros baseado em um filtro
func (s *SQLStore[T]) UpdateMany(ctx context.Context, fd []EntityFieldsToUpdate) (*BulkWriteResult, error) {
	if len(fd) == 0 {
		return nil, fmt.Errorf("nenhum update fornecido")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("erro ao iniciar transação: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()

	now := time.Now()
	var totalMatched, totalModified int64

	for i, fb := range fd {
		if len(fb.Filter) == 0 {
			tx.Rollback()
			return nil, fmt.Errorf("filtro é obrigatório para update %d", i)
		}

		if len(fb.Fields) == 0 {
			tx.Rollback()
			return nil, fmt.Errorf("campos para atualização são obrigatórios para update %d", i)
		}

		// Constrói SET clause
		setClauses := make([]string, 0, len(fb.Fields)+1)
		setValues := make([]any, 0, len(fb.Fields)+1)

		// Ordena as chaves para consistência
		fieldKeys := make([]string, 0, len(fb.Fields))
		for key := range fb.Fields {
			fieldKeys = append(fieldKeys, key)
		}
		sort.Strings(fieldKeys)

		for _, key := range fieldKeys {
			setClauses = append(setClauses, fmt.Sprintf("%s = ?", key))
			setValues = append(setValues, fb.Fields[key])
		}

		// Adiciona updated_at automaticamente
		setClauses = append(setClauses, "updated_at = ?")
		setValues = append(setValues, now)

		// Constrói WHERE clause
		whereClause, whereValues := s.buildWhereClause(fb.Filter)

		// Monta a query completa
		query := fmt.Sprintf(
			"UPDATE %s SET %s%s",
			s.tableName,
			strings.Join(setClauses, ", "),
			whereClause,
		)

		// Combina valores: SET values + WHERE values
		allValues := append(setValues, whereValues...)

		result, err := tx.ExecContext(ctx, query, allValues...)
		if err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("erro ao executar update %d: %w", i, err)
		}

		rowsAffected, _ := result.RowsAffected()
		totalMatched += rowsAffected
		totalModified += rowsAffected
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("erro ao fazer commit: %w", err)
	}

	return &BulkWriteResult{
		MatchedCount:  totalMatched,
		ModifiedCount: totalModified,
	}, nil
}

// Upsert cria ou atualiza um registro
func (s *SQLStore[T]) Upsert(ctx context.Context, e *T, f []StoreUpsertFilter) (*UpdateResult, error) {
	v := reflect.ValueOf(e).Elem()

	// Verifica se existe campo updated_at
	hasUpdatedAt := v.FieldByName("UpdatedAt").IsValid()

	// Preparar campos
	fields := make([]string, 0)
	placeholders := make([]string, 0)
	updates := make([]string, 0)
	values := make([]any, 0)

	if len(f) == 0 {
		f = []StoreUpsertFilter{
			{
				UpsertFieldKey: s.primaryKey,
				UpsertBsonKey:  "ID",
			},
		}
	}

	// Construir lista de campos de conflito (upsert) a partir dos filtros
	conflictFields := make([]string, 0, len(f))
	conflictFieldsMap := make(map[string]bool)
	for _, filter := range f {
		fieldKey := filter.UpsertFieldKey
		if fieldKey == "" {
			fieldKey = s.primaryKey
		}
		conflictFields = append(conflictFields, fieldKey)
		conflictFieldsMap[fieldKey] = true
	}

	for i := range v.NumField() {
		field := v.Type().Field(i)
		fieldName := field.Tag.Get("db")

		if field.Tag.Get("db") != "-" {
			fields = append(fields, fieldName)
			placeholders = append(placeholders, "?")
			values = append(values, v.Field(i).Interface())

			// Campos para atualização (exceto os campos de conflito)
			if !conflictFieldsMap[fieldName] {
				updates = append(updates, fmt.Sprintf("%s = VALUES(%s)", fieldName, fieldName))
			}
		}
	}

	if hasUpdatedAt {
		updates = append(updates, fmt.Sprintf("%s = ?", "updated_at"))
		values = append(values, time.Now())
	}

	var query string
	driverName := s.driver
	switch driverName {
	case enum.DatabaseDriverMysql, enum.DatabaseDriverMariaDB:
		query = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
			s.tableName,
			strings.Join(fields, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(updates, ", "),
		)
	case enum.DatabaseDriverSqlite:
		query = fmt.Sprintf(
			"INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
			s.tableName,
			strings.Join(fields, ", "),
			strings.Join(placeholders, ", "),
		)
	case enum.DatabaseDriverPostgres:
		// PostgreSQL suporta múltiplos campos de conflito
		query = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
			s.tableName,
			strings.Join(fields, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(conflictFields, ", "),
			strings.Join(updates, ", "),
		)
	case enum.DatabaseDriverOracle:
		// Oracle usa MERGE para upsert com múltiplos campos de conflito
		// Construir condições ON para o MERGE
		onConditions := make([]string, 0, len(conflictFields))
		for _, field := range conflictFields {
			onConditions = append(onConditions, fmt.Sprintf("t.%s = ?", field))
		}

		// Construir UPDATE SET (excluindo campos de conflito)
		updateSets := make([]string, 0)
		for _, field := range fields {
			if !conflictFieldsMap[field] {
				updateSets = append(updateSets, fmt.Sprintf("t.%s = ?", field))
			}
		}

		if hasUpdatedAt {
			updateSets = append(updateSets, "t.updated_at = ?")
		}

		query = fmt.Sprintf(
			"MERGE INTO %s t USING dual ON (%s) "+
				"WHEN MATCHED THEN UPDATE SET %s "+
				"WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
			s.tableName,
			strings.Join(onConditions, " AND "),
			strings.Join(updateSets, ", "),
			strings.Join(fields, ", "),
			strings.Join(placeholders, ", "),
		)

		// Para Oracle, precisamos duplicar os valores:
		// 1. Para a cláusula ON (conflictFields)
		// 2. Para o UPDATE SET (campos não-conflito)
		// 3. Para o INSERT VALUES (todos os campos)
		oracleValues := make([]any, 0)

		// Valores para ON condition (conflictFields)
		for _, field := range conflictFields {
			for i := range v.NumField() {
				if v.Type().Field(i).Tag.Get("db") == field {
					oracleValues = append(oracleValues, v.Field(i).Interface())
					break
				}
			}
		}

		// Valores para UPDATE SET (campos não-conflito)
		for _, field := range fields {
			if !conflictFieldsMap[field] {
				for i := range v.NumField() {
					if v.Type().Field(i).Tag.Get("db") == field {
						oracleValues = append(oracleValues, v.Field(i).Interface())
						break
					}
				}
			}
		}

		if hasUpdatedAt {
			oracleValues = append(oracleValues, time.Now())
		}

		// Valores para INSERT (todos os campos)
		oracleValues = append(oracleValues, values...)

		values = oracleValues
	default:
		return nil, fmt.Errorf("unsupported database driver to execute Upsert: %s", driverName.GetValue())
	}

	result, err := s.db.ExecContext(ctx, query, values...)
	if err != nil {
		return nil, err
	}

	rowsAffected, _ := result.RowsAffected()
	return &UpdateResult{UpsertedCount: rowsAffected}, nil
}

// UpsertMany cria ou atualiza múltiplos registros
func (s *SQLStore[T]) UpsertMany(ctx context.Context, entities []T, f []StoreUpsertFilter) (*BulkWriteResult, error) {
	if len(entities) == 0 {
		return nil, nil
	}

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

	for _, entity := range entities {
		v := reflect.ValueOf(&entity).Elem()

		// Verifica se é um novo registro
		idField := v.FieldByName("ID")
		isNewRecord := false
		if idField.IsValid() {
			switch idField.Kind() {
			case reflect.Int, reflect.Int64, reflect.Int32:
				isNewRecord = idField.Int() == 0
			case reflect.String:
				isNewRecord = idField.String() == ""
			}
		}

		// Preparar campos
		fields := make([]string, 0)
		placeholders := make([]string, 0)
		updates := make([]string, 0)
		values := make([]any, 0)

		if len(f) == 0 {
			f = []StoreUpsertFilter{
				{
					UpsertFieldKey: s.primaryKey,
					UpsertBsonKey:  "ID",
				},
			}
		}

		// Construir lista de campos de conflito (upsert) a partir dos filtros
		conflictFields := make([]string, 0, len(f))
		conflictFieldsMap := make(map[string]bool)
		for _, filter := range f {
			fieldKey := filter.UpsertFieldKey
			if fieldKey == "" {
				fieldKey = s.primaryKey
			}
			conflictFields = append(conflictFields, fieldKey)
			conflictFieldsMap[fieldKey] = true
		}

		for i := range v.NumField() {
			field := v.Type().Field(i)
			fieldName := field.Tag.Get("db")

			if fieldName == "-" {
				continue
			}

			// Para novos registros com autoincrement, pula o campo ID
			if isNewRecord && s.autoincrement && fieldName == s.primaryKey {
				continue
			}

			fields = append(fields, fieldName)
			placeholders = append(placeholders, "?")
			values = append(values, v.Field(i).Interface())

			// Campos para atualização (exceto os campos de conflito)
			if !conflictFieldsMap[fieldName] {
				updates = append(updates, fmt.Sprintf("%s = VALUES(%s)", fieldName, fieldName))
			}
		}

		// Verifica se existe campo updated_at
		hasUpdatedAt := v.FieldByName("UpdatedAt").IsValid()
		if hasUpdatedAt {
			updates = append(updates, fmt.Sprintf("%s = ?", "updated_at"))
			values = append(values, time.Now())
		}

		var query string
		switch s.driver {
		case enum.DatabaseDriverMysql, enum.DatabaseDriverMariaDB:
			query = fmt.Sprintf(
				"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
				s.tableName,
				strings.Join(fields, ", "),
				strings.Join(placeholders, ", "),
				strings.Join(updates, ", "),
			)
		case enum.DatabaseDriverSqlite:
			if isNewRecord && s.autoincrement {
				// Para novos registros, usa INSERT simples
				query = fmt.Sprintf(
					"INSERT INTO %s (%s) VALUES (%s)",
					s.tableName,
					strings.Join(fields, ", "),
					strings.Join(placeholders, ", "),
				)
			} else {
				query = fmt.Sprintf(
					"INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
					s.tableName,
					strings.Join(fields, ", "),
					strings.Join(placeholders, ", "),
				)
			}
		case enum.DatabaseDriverPostgres:
			// PostgreSQL suporta múltiplos campos de conflito
			query = fmt.Sprintf(
				"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
				s.tableName,
				strings.Join(fields, ", "),
				strings.Join(placeholders, ", "),
				strings.Join(conflictFields, ", "),
				strings.Join(updates, ", "),
			)
		case enum.DatabaseDriverOracle:
			// Oracle usa MERGE para upsert com múltiplos campos de conflito
			// Construir condições ON para o MERGE
			onConditions := make([]string, 0, len(conflictFields))
			for _, field := range conflictFields {
				onConditions = append(onConditions, fmt.Sprintf("t.%s = ?", field))
			}

			// Construir UPDATE SET (excluindo campos de conflito)
			updateSets := make([]string, 0)
			for _, field := range fields {
				if !conflictFieldsMap[field] {
					updateSets = append(updateSets, fmt.Sprintf("t.%s = ?", field))
				}
			}

			if hasUpdatedAt {
				updateSets = append(updateSets, "t.updated_at = ?")
			}

			query = fmt.Sprintf(
				"MERGE INTO %s t USING dual ON (%s) "+
					"WHEN MATCHED THEN UPDATE SET %s "+
					"WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
				s.tableName,
				strings.Join(onConditions, " AND "),
				strings.Join(updateSets, ", "),
				strings.Join(fields, ", "),
				strings.Join(placeholders, ", "),
			)

			// Para Oracle, precisamos duplicar os valores:
			// 1. Para a cláusula ON (conflictFields)
			// 2. Para o UPDATE SET (campos não-conflito)
			// 3. Para o INSERT VALUES (todos os campos)
			oracleValues := make([]any, 0)

			// Valores para ON condition (conflictFields)
			for _, field := range conflictFields {
				for i := range v.NumField() {
					if v.Type().Field(i).Tag.Get("db") == field {
						oracleValues = append(oracleValues, v.Field(i).Interface())
						break
					}
				}
			}

			// Valores para UPDATE SET (campos não-conflito)
			for _, field := range fields {
				if !conflictFieldsMap[field] {
					for i := range v.NumField() {
						if v.Type().Field(i).Tag.Get("db") == field {
							oracleValues = append(oracleValues, v.Field(i).Interface())
							break
						}
					}
				}
			}

			if hasUpdatedAt {
				oracleValues = append(oracleValues, time.Now())
			}

			// Valores para INSERT (todos os campos)
			oracleValues = append(oracleValues, values...)

			values = oracleValues
		default:
			tx.Rollback()
			return nil, fmt.Errorf("unsupported database driver to execute Upsert: %s", s.driver.GetValue())
		}

		_, err := tx.ExecContext(ctx, query, values...)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("erro ao fazer commit: %w", err)
	}

	return &BulkWriteResult{UpsertedCount: int64(len(entities))}, nil
}

// Delete remove um registro pelo ID
func (s *SQLStore[T]) Delete(ctx context.Context, id any) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", s.tableName, s.primaryKey)
	_, err := s.db.ExecContext(ctx, query, id)
	return err
}

// DeleteOne remove um registro baseado em um filtro
func (s *SQLStore[T]) DeleteOne(ctx context.Context, f map[string]interface{}) error {
	if f == nil || len(f) == 0 {
		return fmt.Errorf("filtro não pode ser nulo ou vazio")
	}

	whereClause, values := s.buildWhereClause(f)
	var query string

	switch s.driver {
	case enum.DatabaseDriverSqlite:
		// SQLite não suporta LIMIT em DELETE, usa subquery com ROWID
		query = fmt.Sprintf("DELETE FROM %s WHERE rowid IN (SELECT rowid FROM %s%s LIMIT 1)",
			s.tableName, s.tableName, whereClause)
	case enum.DatabaseDriverOracle:
		// Oracle não suporta LIMIT, usa ROWNUM em subquery
		query = fmt.Sprintf("DELETE FROM %s WHERE ROWID IN (SELECT ROWID FROM %s%s AND ROWNUM = 1)",
			s.tableName, s.tableName, whereClause)
	case enum.DatabaseDriverMysql, enum.DatabaseDriverMariaDB, enum.DatabaseDriverPostgres:
		// MySQL, MariaDB e PostgreSQL suportam LIMIT em DELETE
		query = fmt.Sprintf("DELETE FROM %s%s LIMIT 1", s.tableName, whereClause)
	default:
		return fmt.Errorf("unsupported database driver for DeleteOne: %s", s.driver.GetValue())
	}

	result, err := s.db.ExecContext(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("erro ao deletar documento: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("erro ao verificar registros deletados: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("nenhum documento encontrado com filtro %v", f)
	}

	return nil
}

// DeleteMany remove múltiplos registros
func (s *SQLStore[T]) DeleteMany(ctx context.Context, f map[string]any) (*DeleteResult, error) {
	whereClause, values := s.buildWhereClause(f)
	query := fmt.Sprintf("DELETE FROM %s", s.tableName)
	query += whereClause

	result, err := s.db.ExecContext(ctx, query, values...)
	if err != nil {
		return nil, err
	}

	rowsAffected, _ := result.RowsAffected()
	return &DeleteResult{DeletedCount: rowsAffected}, nil
}

// func (s *SQLStore[T]) isOracleDriver() bool {
// 	// Para Oracle
// 	var version string
// 	err := s.db.QueryRow("SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'").Scan(&version)
// 	if err == nil && strings.Contains(strings.ToLower(version), "oracle") {
// 		return true
// 	}
//
// 	return false
// }

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
				// Case-insensitive LIKE compatível com todos os bancos
				operator = "ILIKE_COMPAT"
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

		// Tratamento especial para ILIKE compatível com todos os bancos
		if operator == "ILIKE_COMPAT" {
			whereConditions = append(whereConditions, fmt.Sprintf("UPPER(%s) LIKE UPPER(?)", field))
			values = append(values, value)
			continue
		}

		if operator == "IN" {
			// Obter o slice de valores
			valuesSlice, ok := value.([]any)
			if !ok {
				// Tente converter outros tipos de slice
				switch v := value.(type) {
				case []int:
					valuesSlice = make([]any, len(v))
					for i, val := range v {
						valuesSlice[i] = val
					}
				case []string:
					valuesSlice = make([]any, len(v))
					for i, val := range v {
						valuesSlice[i] = val
					}
				// Adicione outros tipos conforme necessário ([]float64, etc.)
				default:
					// Tentar uma última abordagem usando reflection
					rv := reflect.ValueOf(value)
					if rv.Kind() == reflect.Slice {
						valuesSlice = make([]any, rv.Len())
						for i := range rv.Len() {
							valuesSlice[i] = rv.Index(i).Interface()
						}
					} else {
						// Se não for um slice, trate como um valor único
						whereConditions = append(whereConditions, fmt.Sprintf("%s %s (?)", field, operator))
						values = append(values, value)
						continue
					}
				}
			}

			// Criar placeholders para cada valor no slice: (?, ?, ?)
			placeholders := make([]string, len(valuesSlice))
			for i := range valuesSlice {
				placeholders[i] = "?"
			}

			// Construir a condição WHERE com os placeholders
			whereConditions = append(whereConditions, fmt.Sprintf("%s %s (%s)",
				field, operator, strings.Join(placeholders, ", ")))

			// Adicionar cada valor individualmente ao slice de valores
			values = append(values, valuesSlice...)

			continue
		}

		whereConditions = append(whereConditions, fmt.Sprintf("%s %s ?", field, operator))
		values = append(values, value)
	}

	return " WHERE " + strings.Join(whereConditions, " AND "), values
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

			// Converte o valor para o tipo correto
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
	case reflect.Bool:
		switch v := value.(type) {
		case bool:
			field.SetBool(v)
		case int, int64:
			// SQLite armazena boolean como INTEGER (0 ou 1)
			field.SetBool(reflect.ValueOf(v).Int() != 0)
		case []byte:
			// Pode vir como string "0", "1", "true", "false"
			strVal := string(v)
			field.SetBool(strVal == "1" || strVal == "true" || strVal == "TRUE")
		case string:
			field.SetBool(v == "1" || v == "true" || v == "TRUE")
		default:
			// Tenta converter via reflection para int64
			rv := reflect.ValueOf(value)
			switch rv.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				field.SetBool(rv.Int() != 0)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				field.SetBool(rv.Uint() != 0)
			case reflect.Float32, reflect.Float64:
				field.SetBool(rv.Float() != 0)
			}
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
	for i := range v.NumField() {
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
