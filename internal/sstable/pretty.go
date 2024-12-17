package sstable

import (
	"bytes"
	"fmt"
	"github.com/slatedb/slatedb-go/internal/sstable/block"
	"strings"
)

// PrettyPrint returns a string representation of the SSTable in a human-readable format
//
// SSTable Info:
//
//	  First Key: key1
//	  Index Offset: 147
//	  Index Length: 168
//	  Filter Offset: 140
//	  Filter Length: 7
//	  Compression Codec: None
//	Bloom Filter:
//	  Number of Probes: 6
//	  Data Length: 5
//	Blocks:
//	  First Block Offset: 0
//	  End Offset: 140
//	  Block 0:
//	    Offset: 0
//	    FirstKey: []byte("key1")
//	    KeyValues:
//	      Offset: 0
//	          Key: []byte("key1") - 4 bytes
//	        Value: []byte("value1") - 6 bytes
//	  Block 1:
//	    Offset: 35
//	    FirstKey: []byte("key2")
//	    KeyValues:
//	      Offset: 0
//	          Key: []byte("key2") - 4 bytes
//	        Value: []byte("value2") - 6 bytes
//	  Block 2:
//	    Offset: 70
//	    FirstKey: []byte("key3")
//	    KeyValues:
//	      Offset: 0
//	          Key: []byte("key3") - 4 bytes
//	        Value: []byte("value3") - 6 bytes
//	  Block 3:
//	    Offset: 105
//	    FirstKey: []byte("key4")
//	    KeyValues:
//	      Offset: 0
//	          Key: []byte("key4") - 4 bytes
//	        Value: []byte("value4") - 6 bytes
func PrettyPrint(table *Table) string {
	var buf bytes.Buffer

	// Print SSTable Info
	_, _ = fmt.Fprintf(&buf, "SSTable Info:\n")
	_, _ = fmt.Fprintf(&buf, "  First Key: %s\n", string(table.Info.FirstKey))
	_, _ = fmt.Fprintf(&buf, "  Index Offset: %d\n", table.Info.IndexOffset)
	_, _ = fmt.Fprintf(&buf, "  Index Length: %d\n", table.Info.IndexLen)
	_, _ = fmt.Fprintf(&buf, "  Filter Offset: %d\n", table.Info.FilterOffset)
	_, _ = fmt.Fprintf(&buf, "  Filter Length: %d\n", table.Info.FilterLen)
	_, _ = fmt.Fprintf(&buf, "  Compression Codec: %s\n", table.Info.CompressionCodec)

	// Print Bloom Filter info if present
	if filter, ok := table.Bloom.Get(); ok {
		_, _ = fmt.Fprintf(&buf, "Bloom Filter:\n")
		_, _ = fmt.Fprintf(&buf, "  Number of Probes: %d\n", filter.NumProbes)
		_, _ = fmt.Fprintf(&buf, "  Data Length: %d\n", len(filter.Data))
	} else {
		_, _ = fmt.Fprintf(&buf, "Bloom Filter:\n")
		_, _ = fmt.Fprintf(&buf, "  No Bloom Filter\n")
	}

	encoded := EncodeTable(table)

	index, err := ReadIndexRaw(table.Info, encoded)
	if err != nil {
		buf.WriteString(fmt.Sprintf("ERROR: while parsing index at [%d:%d] - %s\n",
			table.Info.IndexOffset, table.Info.IndexLen, err.Error()))
		return buf.String()
	}

	blocksMeta := index.BlockMeta()
	_, _ = fmt.Fprintf(&buf, "Blocks:\n")
	_, _ = fmt.Fprintf(&buf, "  First Block Offset: %d\n", blocksMeta[0].Offset)
	_, _ = fmt.Fprintf(&buf, "  End Offset: %d\n", table.Info.FilterOffset)

	for i, meta := range blocksMeta {
		_, _ = fmt.Fprintf(&buf, "  Block %d:\n", i)
		_, _ = fmt.Fprintf(&buf, "    Offset: %d\n", meta.Offset)
		_, _ = fmt.Fprintf(&buf, "    FirstKey: []byte(\"%s\")\n", meta.FirstKey)
		_, _ = fmt.Fprintf(&buf, "    KeyValues:\n")

		blk, err := ReadBlockRaw(table.Info, index, uint64(i), encoded)
		if err != nil {
			buf.WriteString(fmt.Sprintf("ERROR: while parsing block at offset %d - %s\n",
				meta.Offset, err.Error()))
			return buf.String()
		}
		_, _ = fmt.Fprintf(&buf, "%s\n", indent(6, block.PrettyPrint(blk)))
	}
	return strings.TrimRight(buf.String(), "\n")
}

// prefix each line delimited by '\n' by X number of spaces
func indent(indent int, input string) string {
	prefix := strings.Repeat(" ", indent)
	lines := strings.Split(input, "\n")
	for i, line := range lines {
		if line != "" {
			lines[i] = prefix + line
		}
	}
	return strings.TrimRight(strings.Join(lines, "\n"), "\n")
}
