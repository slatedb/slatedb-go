package logger

import (
	"go.uber.org/zap"
)

var _logger *zap.Logger

func Init() {
	var err error
	_logger, err = zap.NewProduction()
	if err != nil {
		panic(err)
	}
}

func Sync() {
	if _logger != nil {
		_ = _logger.Sync()
	}
}

func Error(message string, field ...zap.Field) {
	_logger.Error(message, field...)
}

func Warn(message string, field ...zap.Field) {
	_logger.Warn(message, field...)
}

func Info(message string, field ...zap.Field) {
	_logger.Info(message, field...)
}
