package sstable

//type Decoder struct {
//	conf Config
//}
//
//func (f *Decoder) ReadInfo(obj common.ReadOnlyBlob) (*Info, error) {
//	size, err := obj.Len()
//	if err != nil {
//		return nil, err
//	}
//	if size <= 4 {
//		return nil, common.ErrEmptySSTable
//	}
//
//	// Get the metadata. Last 4 bytes are the metadata offset of SsTableInfo
//	offsetIndex := uint64(size - 4)
//	offsetBytes, err := obj.ReadRange(common.Range{Start: offsetIndex, End: uint64(size)})
//	if err != nil {
//		return nil, err
//	}
//
//	metadataOffset := binary.BigEndian.Uint32(offsetBytes)
//	metadataBytes, err := obj.ReadRange(common.Range{Start: uint64(metadataOffset), End: offsetIndex})
//	if err != nil {
//		return nil, err
//	}
//
//	return DecodeBytesToSSTableInfo(metadataBytes, f.conf.Compression)
//}
