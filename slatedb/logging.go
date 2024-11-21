package slatedb

import "log/slog"

const (
	// LevelDebug includes debug logging outside of warnings and errors
	LevelDebug = slog.LevelDebug
	// LevelVerbose includes LevelDebug and additional verbose output
	LevelVerbose = slog.LevelDebug + 1
	LevelError   = slog.LevelError
	LevelWarn    = slog.LevelWarn
)

// optionsToAttr converts the current database options to slog attributes
func optionsToAttr(opts DBOptions) []slog.Attr {
	attrs := []slog.Attr{
		slog.Int("MinFilterKeys", int(opts.MinFilterKeys)),
		slog.Duration("ManifestPollInterval", opts.ManifestPollInterval),
		slog.Duration("FlushInterval", opts.FlushInterval),
		slog.Int("L0SSTSizeBytes", int(opts.L0SSTSizeBytes)),
		slog.String("CompressionCodec", opts.CompressionCodec.String()),
	}
	if opts.CompactorOptions.Enabled {
		attrs = append(attrs,
			slog.Bool("Compactor.Enabled", true),
			slog.Duration("Compactor.PollInterval", opts.CompactorOptions.PollInterval),
			slog.Int("Compactor.MaxSSTSize", int(opts.CompactorOptions.MaxSSTSize)))
	}
	return attrs
}
