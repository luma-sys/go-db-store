package mongo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCreateDatePeriodFilter(t *testing.T) {
	tests := []struct {
		name     string
		start    time.Time
		end      time.Time
		expected bson.M
	}{
		{
			name:     "deve retornar nil quando start e end são zero",
			start:    time.Time{},
			end:      time.Time{},
			expected: nil,
		},
		{
			name:     "deve criar filtro apenas com start",
			start:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:      time.Time{},
			expected: bson.M{"$gte": bson.NewDateTimeFromTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))},
		},
		{
			name:     "deve criar filtro apenas com end",
			start:    time.Time{},
			end:      time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
			expected: bson.M{"$lte": bson.NewDateTimeFromTime(time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC))},
		},
		{
			name:  "deve criar filtro com start e end",
			start: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:   time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
			expected: bson.M{
				"$gte": bson.NewDateTimeFromTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
				"$lte": bson.NewDateTimeFromTime(time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CreateDatePeriodFilter(tt.start, tt.end)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCreateDatePeriodFilterMap(t *testing.T) {
	tests := []struct {
		name     string
		start    time.Time
		end      time.Time
		expected bson.M
	}{
		{
			name:     "deve retornar nil quando start e end são zero",
			start:    time.Time{},
			end:      time.Time{},
			expected: nil,
		},
		{
			name:     "deve criar filtro apenas com start",
			start:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:      time.Time{},
			expected: bson.M{"$gte": bson.NewDateTimeFromTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))},
		},
		{
			name:     "deve criar filtro apenas com end",
			start:    time.Time{},
			end:      time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
			expected: bson.M{"$lte": bson.NewDateTimeFromTime(time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC))},
		},
		{
			name:  "deve criar filtro com start e end",
			start: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:   time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
			expected: bson.M{
				"$gte": bson.NewDateTimeFromTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
				"$lte": bson.NewDateTimeFromTime(time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CreateDatePeriodFilterMap(tt.start, tt.end)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCreateInFilter(t *testing.T) {
	tests := []struct {
		name     string
		values   []string
		expected bson.M
	}{
		{
			name:     "deve retornar nil quando values está vazio",
			values:   []string{},
			expected: nil,
		},
		{
			name:     "deve criar filtro in com um valor",
			values:   []string{"value1"},
			expected: bson.M{"$in": []string{"value1"}},
		},
		{
			name:     "deve criar filtro in com múltiplos valores",
			values:   []string{"value1", "value2", "value3"},
			expected: bson.M{"$in": []string{"value1", "value2", "value3"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CreateInFilter(tt.values)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCreateInFilterWithField(t *testing.T) {
	tests := []struct {
		name     string
		field    string
		values   []string
		expected bson.M
	}{
		{
			name:     "deve retornar nil quando values está vazio",
			field:    "testField",
			values:   []string{},
			expected: nil,
		},
		{
			name:     "deve criar filtro in com campo e um valor",
			field:    "testField",
			values:   []string{"value1"},
			expected: bson.M{"testField": bson.M{"$in": []string{"value1"}}},
		},
		{
			name:     "deve criar filtro in com campo e múltiplos valores",
			field:    "testField",
			values:   []string{"value1", "value2", "value3"},
			expected: bson.M{"testField": bson.M{"$in": []string{"value1", "value2", "value3"}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CreateInFilterWithField(tt.field, tt.values)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCreateLikeFilter(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected bson.M
	}{
		{
			name:     "deve criar filtro like com valor simples",
			value:    "test",
			expected: bson.M{"$regex": "test", "$options": "i"},
		},
		{
			name:     "deve criar filtro like com string vazia",
			value:    "",
			expected: bson.M{"$regex": "", "$options": "i"},
		},
		{
			name:     "deve criar filtro like com caracteres especiais",
			value:    "test*123",
			expected: bson.M{"$regex": "test*123", "$options": "i"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CreateLikeFilter(tt.value)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCreateLikeFilters(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		fields   []string
		expected []bson.D
	}{
		{
			name:     "deve retornar nil quando fields está vazio",
			value:    "test",
			fields:   []string{},
			expected: nil,
		},
		{
			name:   "deve criar filtro like para um campo",
			value:  "test",
			fields: []string{"field1"},
			expected: []bson.D{
				{{Key: "field1", Value: bson.M{"$regex": "test", "$options": "i"}}},
			},
		},
		{
			name:   "deve criar filtros like para múltiplos campos",
			value:  "test",
			fields: []string{"field1", "field2", "field3"},
			expected: []bson.D{
				{{Key: "field1", Value: bson.M{"$regex": "test", "$options": "i"}}},
				{{Key: "field2", Value: bson.M{"$regex": "test", "$options": "i"}}},
				{{Key: "field3", Value: bson.M{"$regex": "test", "$options": "i"}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CreateLikeFilters(tt.value, tt.fields)
			assert.Equal(t, tt.expected, result)
		})
	}
}
