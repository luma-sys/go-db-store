package enum

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
)

type DatabaseDriver string

const (
	DatabaseDriverOracle   DatabaseDriver = "oracle"
	DatabaseDriverPostgres DatabaseDriver = "postgres"
	DatabaseDriverMysql    DatabaseDriver = "mysql"
	DatabaseDriverSqlite   DatabaseDriver = "sqlite"
	DatabaseDriverMariaDB  DatabaseDriver = "mariadb"
)

// AllDatabaseDriver retorna todos os drivers disponíveis
var AllDatabaseDriver = []DatabaseDriver{
	DatabaseDriverOracle,
	DatabaseDriverPostgres,
	DatabaseDriverMysql,
	DatabaseDriverSqlite,
	DatabaseDriverMariaDB,
}

// IsValid verifica se o status é válido
func (s DatabaseDriver) IsValid() bool {
	return slices.Contains(AllDatabaseDriver, s)
}

// UnmarshalJSON implementa a interface json.Unmarshaler
func (s *DatabaseDriver) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}

	status := DatabaseDriver(str)
	if !status.IsValid() {
		return errors.New("driver inválido")
	}

	*s = status
	return nil
}

// GetValue retorna a descrição do drivers
func (s DatabaseDriver) GetValue() string {
	switch s {
	case DatabaseDriverOracle:
		return "oracle"
	case DatabaseDriverPostgres:
		return "postgres"
	case DatabaseDriverMysql:
		return "mysql"
	case DatabaseDriverSqlite:
		return "sqlite"
	case DatabaseDriverMariaDB:
		return "mariadb"
	default:
		return ""
	}
}

// FromString Implementação da interface StringConverter para DatabaseDriver
func (s *DatabaseDriver) FromString(str string) (any, error) {
	status, err := ParseDatabaseDriver(str)
	if err != nil {
		return nil, err
	}
	*s = status
	return s, nil
}

// ParseDatabaseDriver recebe uma string e retorna o driver
func ParseDatabaseDriver(s string) (DatabaseDriver, error) {
	normalized := strings.ToLower(strings.TrimSpace(s))

	switch normalized {
	case "oracle":
		return DatabaseDriverOracle, nil
	case "postgres":
		return DatabaseDriverPostgres, nil
	case "mysql":
		return DatabaseDriverMysql, nil
	case "sqlite":
		return DatabaseDriverSqlite, nil
	case "mariadb":
		return DatabaseDriverMariaDB, nil
	default:
		return "", errors.New("driver inválido")
	}
}
