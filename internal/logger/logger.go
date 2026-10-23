package logger

import (
	"log/slog"
	"os"
)

func New(service string) *slog.Logger {
	handler := slog.NewJSONHandler(
		os.Stdout,
		nil,
	)

	return slog.New(handler).With(
		slog.String("service", service),
	)
}
