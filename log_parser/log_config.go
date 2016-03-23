package log_parser

import (
	"fmt"
	"strings"

	"github.com/fizx/logs"
)

// LogConfig contains configuration information for logging
type LogConfig struct {
	File  string `json:"file,omitempty"`
	Level string `json:"level,omitempty"`
}

// ParseConfig contains configuration infomation for parse log lines
type ParseConfig struct {
	KeysToIgnore   []string `json:"keys_to_delete,omitempty"`
	IgnoreDefaults bool     `json:"ignore_defaults,omitempty"`
	Pattern        string   `json:"pattern,omitempty"`
	TimePatterns   []string `json:"time_patterns,omitempty"`
}

// TailConfig contains configuation  how to tail
type TailConfig struct {
	FromBeginning bool `json:"from_beginning,omitempty"`
	ReOpen        bool `json:"reopen,omitempty"` // Reopen recreated files (tail -F)
}

// StringToLogLevel converts a string to a log leve
func StringToLogLevel(config string) (level logs.Level, err error) {
	uconfig := strings.ToUpper(config)
	if uconfig == "DEBUG" {
		level = logs.DEBUG
		return
	}
	if uconfig == "INFO" {
		level = logs.INFO
		return
	}
	if uconfig == "WARN" {
		level = logs.WARN
		return
	}
	if uconfig == "ERROR" {
		level = logs.ERROR
		return
	}
	if uconfig == "FATAL" {
		level = logs.FATAL
		return
	}
	err = fmt.Errorf("Invalid log level specification: %s", config)
	return
}
