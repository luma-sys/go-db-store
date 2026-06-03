package store

type IndexDirection int

const (
	ASC  IndexDirection = 1
	DESC IndexDirection = -1
)

type IndexField struct {
	Key string
	Dir IndexDirection
}

type IndexDefinition struct {
	fields []IndexField
	unique bool
	sparse bool
	name   string
}

func NewIndex(keys ...string) *IndexDefinition {
	var fields []IndexField
	for _, k := range keys {
		fields = append(fields, IndexField{Key: k, Dir: ASC})
	}

	return &IndexDefinition{fields: fields}
}

func (b *IndexDefinition) Asc() *IndexDefinition {
	for i := range b.fields {
		b.fields[i].Dir = ASC
	}
	return b
}

func (b *IndexDefinition) Desc() *IndexDefinition {
	for i := range b.fields {
		b.fields[i].Dir = DESC
	}
	return b
}

func (b *IndexDefinition) Fields(fields ...IndexField) *IndexDefinition {
	b.fields = fields
	return b
}

func (b *IndexDefinition) Optional() *IndexDefinition {
	b.sparse = true
	return b
}

func (b *IndexDefinition) Unique() *IndexDefinition {
	b.unique = true
	return b
}

func (b *IndexDefinition) Name(name string) *IndexDefinition {
	b.name = name
	return b
}
