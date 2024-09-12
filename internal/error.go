package internal

import "errors"

var (
	IoError                 = errors.New("IO error")
	ChecksumMismatch        = errors.New("checksum mismatch")
	EmptySSTable            = errors.New("empty SSTable")
	EmptyBlockMeta          = errors.New("empty block metadata")
	EmptyBlock              = errors.New("empty block")
	ObjectStoreError        = errors.New("object store error")
	ManifestVersionExists   = errors.New("manifest file already exists")
	InvalidFlatbuffer       = errors.New("invalid sst error")
	InvalidDBState          = errors.New("invalid DB state error")
	InvalidCompaction       = errors.New("invalid compaction")
	Fenced                  = errors.New("detected newer DB client")
	InvalidCompressionCodec = errors.New("invalid compression codec")
	BlockDecompressionError = errors.New("error Decompressing Block")
	BlockCompressionError   = errors.New("error Compressing Block")
)
