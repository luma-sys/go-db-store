package nanoid

import gonanoid "github.com/matoous/go-nanoid/v2"

func mustCreateID(size int) string {
	return gonanoid.MustGenerate("123456789ABCDEFGHIJKLMNPQRSTUVWXYZ", size)
}

func New() string {
	return mustCreateID(18)
}

func NewTiny() string {
	return mustCreateID(6)
}
