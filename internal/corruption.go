package internal

import (
	"fmt"
)

type CorruptionKind int

// String returns the kind as a string
func (k CorruptionKind) String() string {
	switch k {
	case KindWAL:
		return "WAL"
	case KindCompacted:
		return "Compacted"
	case KindManifest:
		return "Manifest"
	case KindFileNotFound:
		return "FileNotFound"
	case KindStoreChecksum:
		return "StoreChecksum"
	default:
		return "Unknown"
	}
}

// A list of all the possible kinds of corruption
const (
	KindWAL CorruptionKind = iota
	KindCompacted
	KindManifest
	KindFileNotFound
	KindStoreChecksum
)

type CorruptionDetails struct {
	// Kind is the kind of corruption which occurred
	Kind CorruptionKind
	// Message is the error message reported when corruption was detected
	Message string
	// Path to the file which is corrupted
	Path string
}

// String returns a string including all the details of the corruption
func (d CorruptionDetails) String() string {
	return fmt.Sprintf("Corruption detected: Kind=%s, Message=%s, Path=%s", d.Kind, d.Message, d.Path)
}
