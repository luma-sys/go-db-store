package nanoid

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name           string
		expectedLength int
		expectedRegex  string
		runTimes       int
	}{
		{
			name:           "deve gerar ID com 18 caracteres",
			expectedLength: 18,
			expectedRegex:  "^[123456789ABCDEFGHIJKLMNPQRSTUVWXYZ]{18}$",
			runTimes:       100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Executa várias vezes para garantir consistência
			for i := 0; i < tt.runTimes; i++ {
				result := New()

				// Verifica o tamanho
				assert.Equal(t, tt.expectedLength, len(result))

				// Verifica o formato
				matched, err := regexp.MatchString(tt.expectedRegex, result)
				assert.NoError(t, err)
				assert.True(t, matched)

				// Verifica se não contém caracteres inválidos
				invalidChars := regexp.MustCompile("[^123456789ABCDEFGHIJKLMNPQRSTUVWXYZ]")
				assert.False(t, invalidChars.MatchString(result))
			}
		})
	}
}

func TestNewTiny(t *testing.T) {
	tests := []struct {
		name           string
		expectedLength int
		expectedRegex  string
		runTimes       int
	}{
		{
			name:           "deve gerar ID tiny com 6 caracteres",
			expectedLength: 6,
			expectedRegex:  "^[123456789ABCDEFGHIJKLMNPQRSTUVWXYZ]{6}$",
			runTimes:       100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Executa várias vezes para garantir consistência
			for i := 0; i < tt.runTimes; i++ {
				result := NewTiny()

				// Verifica o tamanho
				assert.Equal(t, tt.expectedLength, len(result))

				// Verifica o formato
				matched, err := regexp.MatchString(tt.expectedRegex, result)
				assert.NoError(t, err)
				assert.True(t, matched)

				// Verifica se não contém caracteres inválidos
				invalidChars := regexp.MustCompile("[^123456789ABCDEFGHIJKLMNPQRSTUVWXYZ]")
				assert.False(t, invalidChars.MatchString(result))
			}
		})
	}
}

func TestUniqueness(t *testing.T) {
	tests := []struct {
		name      string
		generator func() string
		numIDs    int
	}{
		{
			name:      "deve gerar IDs únicos com New()",
			generator: New,
			numIDs:    1000,
		},
		{
			name:      "deve gerar IDs únicos com NewTiny()",
			generator: NewTiny,
			numIDs:    1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mapa para verificar duplicatas
			ids := make(map[string]bool)

			// Gera vários IDs
			for i := 0; i < tt.numIDs; i++ {
				id := tt.generator()

				// Verifica se o ID já existe
				_, exists := ids[id]
				assert.False(t, exists, "ID duplicado encontrado: %s", id)

				// Adiciona o ID ao mapa
				ids[id] = true
			}

			// Verifica se o número de IDs únicos é igual ao número de IDs gerados
			assert.Equal(t, tt.numIDs, len(ids))
		})
	}
}

func BenchmarkNew(b *testing.B) {
	for i := 0; i < b.N; i++ {
		New()
	}
}

func BenchmarkNewTiny(b *testing.B) {
	for i := 0; i < b.N; i++ {
		NewTiny()
	}
}
