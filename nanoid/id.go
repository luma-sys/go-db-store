package nanoid

import gonanoid "github.com/matoous/go-nanoid/v2"

func createID(size int) string {
	return gonanoid.MustGenerate("123456789ABCDEFGHIJKLMNPQRSTUVWXYZ", size)
}

// New generate a nanoid of 18 chars
func New() string {
	return createID(18)
}

// NewTiny generate a nanoid of 6 chars
func NewTiny() string {
	return createID(6)
}
