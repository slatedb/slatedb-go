package slatedb

import "errors"

var (
	ErrIo                      = errors.New("IO error")
	ErrChecksumMismatch        = errors.New("checksum mismatch")
	ErrEmptySSTable            = errors.New("empty SSTable")
	ErrEmptyBlockMeta          = errors.New("empty block metadata")
	ErrEmptyBlock              = errors.New("empty block")
	ErrObjectStore             = errors.New("object store error")
	ErrManifestVersionExists   = errors.New("manifest file already exists")
	ErrInvalidFlatbuffer       = errors.New("invalid sst error")
	ErrInvalidDBState          = errors.New("invalid DB state error")
	ErrInvalidCompaction       = errors.New("invalid compaction")
	ErrFenced                  = errors.New("detected newer DB client")
	ErrInvalidCompressionCodec = errors.New("invalid compression codec")
	ErrBlockDecompression      = errors.New("error Decompressing Block")
	ErrBlockCompression        = errors.New("error Compressing Block")
	ErrReadBlocks              = errors.New("error Reading Blocks")
)
