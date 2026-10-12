package logging

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"regexp"
)

var Level slog.LevelVar

var logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: &Level}))
var urls = regexp.MustCompile(`(?i)\b(?:rtmp|https?)://[^\s'"<>]+`)
var secrets = regexp.MustCompile(`(?i)\b(token|password|secret(?:_?key|_?id)?|access_?key(?:_?id)?|authorization|signature|session_?token|api_?key)["']?\s*[=:]\s*(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|(?:bearer|basic)\s+[^\s,;]+|[^\s,;]+)`)

func redact(message string) string {
	message = urls.ReplaceAllStringFunc(message, func(raw string) string {
		address, err := url.Parse(raw)
		if err != nil {
			return "[redacted URL]"
		}
		address.User = nil
		if address.RawQuery != "" {
			address.RawQuery = "redacted"
		}
		address.Fragment = ""
		return address.String()
	})
	return secrets.ReplaceAllString(message, "$1=[redacted]")
}

func write(level slog.Level, arguments ...any) {
	if logger.Enabled(context.Background(), level) {
		logger.Log(context.Background(), level, redact(fmt.Sprint(arguments...)))
	}
}

func writef(level slog.Level, format string, arguments ...any) {
	if logger.Enabled(context.Background(), level) {
		logger.Log(context.Background(), level, redact(fmt.Sprintf(format, arguments...)))
	}
}

func Trace(arguments ...any) { write(slog.LevelDebug-4, arguments...) }
func Debug(arguments ...any) { write(slog.LevelDebug, arguments...) }
func Info(arguments ...any)  { write(slog.LevelInfo, arguments...) }
func Warn(arguments ...any)  { write(slog.LevelWarn, arguments...) }
func Error(arguments ...any) { write(slog.LevelError, arguments...) }

func Tracef(format string, arguments ...any) { writef(slog.LevelDebug-4, format, arguments...) }
func Debugf(format string, arguments ...any) { writef(slog.LevelDebug, format, arguments...) }
func Infof(format string, arguments ...any)  { writef(slog.LevelInfo, format, arguments...) }
func Warnf(format string, arguments ...any)  { writef(slog.LevelWarn, format, arguments...) }
func Errorf(format string, arguments ...any) { writef(slog.LevelError, format, arguments...) }
