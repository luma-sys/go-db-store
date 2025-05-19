package enum

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatabaseDriver_IsValid(t *testing.T) {
	tests := []struct {
		name     string
		driver   DatabaseDriver
		expected bool
	}{
		{
			name:     "deve validar Oracle como válido",
			driver:   DatabaseDriverOracle,
			expected: true,
		},
		{
			name:     "deve validar Postgres como válido",
			driver:   DatabaseDriverPostgres,
			expected: true,
		},
		{
			name:     "deve validar MySQL como válido",
			driver:   DatabaseDriverMysql,
			expected: true,
		},
		{
			name:     "deve validar SQLite como válido",
			driver:   DatabaseDriverSqlite,
			expected: true,
		},
		{
			name:     "deve validar MariaDB como válido",
			driver:   DatabaseDriverMariaDB,
			expected: true,
		},
		{
			name:     "deve invalidar driver vazio",
			driver:   "",
			expected: false,
		},
		{
			name:     "deve invalidar driver inválido",
			driver:   "invalid_driver",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.driver.IsValid()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDatabaseDriver_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name        string
		json        string
		expected    DatabaseDriver
		expectError bool
	}{
		{
			name:        "deve deserializar Oracle",
			json:        `"oracle"`,
			expected:    DatabaseDriverOracle,
			expectError: false,
		},
		{
			name:        "deve deserializar Postgres",
			json:        `"postgres"`,
			expected:    DatabaseDriverPostgres,
			expectError: false,
		},
		{
			name:        "deve falhar com driver inválido",
			json:        `"invalid"`,
			expected:    "",
			expectError: true,
		},
		{
			name:        "deve falhar com JSON inválido",
			json:        `{"invalid": "json"}`,
			expected:    "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var driver DatabaseDriver
			err := json.Unmarshal([]byte(tt.json), &driver)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, driver)
			}
		})
	}
}

func TestDatabaseDriver_GetValue(t *testing.T) {
	tests := []struct {
		name     string
		driver   DatabaseDriver
		expected string
	}{
		{
			name:     "deve retornar descrição Oracle",
			driver:   DatabaseDriverOracle,
			expected: "oracle",
		},
		{
			name:     "deve retornar descrição Postgres",
			driver:   DatabaseDriverPostgres,
			expected: "postgres",
		},
		{
			name:     "deve retornar descrição MySQL",
			driver:   DatabaseDriverMysql,
			expected: "mysql",
		},
		{
			name:     "deve retornar descrição SQLite",
			driver:   DatabaseDriverSqlite,
			expected: "sqlite",
		},
		{
			name:     "deve retornar descrição MariaDB",
			driver:   DatabaseDriverMariaDB,
			expected: "mariadb",
		},
		{
			name:     "deve retornar string vazia para driver inválido",
			driver:   "invalid",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.driver.GetValue()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDatabaseDriver_FromString(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    DatabaseDriver
		expectError bool
	}{
		{
			name:        "deve converter Oracle",
			input:       "oracle",
			expected:    DatabaseDriverOracle,
			expectError: false,
		},
		{
			name:        "deve converter POSTGRES maiúsculo",
			input:       "POSTGRES",
			expected:    DatabaseDriverPostgres,
			expectError: false,
		},
		{
			name:        "deve converter mysql minúsculo",
			input:       "mysql",
			expected:    DatabaseDriverMysql,
			expectError: false,
		},
		{
			name:        "deve converter string com espaços",
			input:       "  sqlite  ",
			expected:    DatabaseDriverSqlite,
			expectError: false,
		},
		{
			name:        "deve falhar com driver inválido",
			input:       "invalid",
			expected:    "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var driver DatabaseDriver
			result, err := driver.FromString(tt.input)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, *result.(*DatabaseDriver))
			}
		})
	}
}

func TestParseDatabaseDriver(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    DatabaseDriver
		expectError bool
	}{
		{
			name:        "deve fazer parse de Oracle",
			input:       "oracle",
			expected:    DatabaseDriverOracle,
			expectError: false,
		},
		{
			name:        "deve fazer parse de POSTGRES maiúsculo",
			input:       "POSTGRES",
			expected:    DatabaseDriverPostgres,
			expectError: false,
		},
		{
			name:        "deve fazer parse de mysql minúsculo",
			input:       "mysql",
			expected:    DatabaseDriverMysql,
			expectError: false,
		},
		{
			name:        "deve fazer parse de string com espaços",
			input:       "  SQLITE  ",
			expected:    DatabaseDriverSqlite,
			expectError: false,
		},
		{
			name:        "deve falhar com driver inválido",
			input:       "invalid",
			expected:    "",
			expectError: true,
		},
		{
			name:        "deve falhar com string vazia",
			input:       "",
			expected:    "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseDatabaseDriver(tt.input)

			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, DatabaseDriver(""), result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestAllDatabaseDriver(t *testing.T) {
	expectedDrivers := []DatabaseDriver{
		DatabaseDriverOracle,
		DatabaseDriverPostgres,
		DatabaseDriverMysql,
		DatabaseDriverSqlite,
		DatabaseDriverMariaDB,
	}

	assert.Equal(t, expectedDrivers, AllDatabaseDriver)
	assert.Len(t, AllDatabaseDriver, 5)
}
