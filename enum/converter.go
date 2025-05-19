package enum

type StringConverter interface {
	// FromString converte uma string para o tipo específico
	FromString(string) (any, error)
}
