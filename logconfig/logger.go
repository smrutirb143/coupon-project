package log

import (
	"fmt"
	"os"
	"sync"
	"time"
)

var GlobalLogger *Logger

// Logger represents a logging instance
type Logger struct {
	mu      sync.Mutex
	logFile *os.File
}

// NewLogger creates a new Logger instance
func OpenLog(logFilePath string) (*Logger, error) {
	file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &Logger{logFile: file}, nil
}

// WriteLog writes log messages to the log file
func (l *Logger) WriteLog(level string, message string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	logLine := fmt.Sprintf("[%s] [%s] %s\n", timestamp, level, message)
	l.logFile.WriteString(logLine)
}

// Close closes the log file
func (l *Logger) Close() {
	l.logFile.Close()
}
