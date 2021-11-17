package proto

type Compression byte

const (
	CompressionDisabled Compression = 0
	CompressionEnabled  Compression = 1
)

func (c Compression) Encode(b *Buffer) {
	b.PutUVarInt(uint64(c))
}
