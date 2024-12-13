package common

type OrderedBytesGenerator struct {
	suffix []byte
	data   []byte
	min    byte
	max    byte
}

func NewOrderedBytesGeneratorWithByteRange(data []byte, min byte, max byte) OrderedBytesGenerator {
	return OrderedBytesGenerator{[]byte{}, data, min, max}
}

func NewOrderedBytesGenerator(suffix, data []byte, min, max byte) OrderedBytesGenerator {
	return OrderedBytesGenerator{suffix, data, min, max}
}

func (g OrderedBytesGenerator) Clone() OrderedBytesGenerator {
	newGen := g
	newGen.suffix = append([]byte{}, g.suffix...)
	newGen.data = append([]byte{}, g.data...)
	return newGen
}

func (g OrderedBytesGenerator) Next() []byte {
	result := make([]byte, 0, len(g.data)+SizeOfUint32)
	result = append(result, g.data...)
	result = append(result, g.suffix...)
	g.increment()
	return result
}

func (g OrderedBytesGenerator) increment() {
	pos := len(g.data) - 1
	for pos >= 0 && g.data[pos] == g.max {
		g.data[pos] = g.min
		pos -= 1
	}
	if pos >= 0 {
		g.data[pos] += 1
	}
}
