package run_test

import (
	"testing"

	"github.com/fizx/logs"
	"github.com/willf/translog/run"
)

var stringToLogLevelTestCases = []struct {
	input      string
	expected   logs.Level
	should_err bool // expected error
}{
	{"debug", logs.DEBUG, false},
	{"Debug", logs.DEBUG, false},
	{"DEBUG", logs.DEBUG, false},
	{"info", logs.INFO, false},
	{"warn", logs.WARN, false},
	{"error", logs.ERROR, false},
	{"fatal", logs.FATAL, false},
	{"decafe", logs.FATAL, true},
}

func TestStringToLogLevel(t *testing.T) {
	for i, testCase := range stringToLogLevelTestCases {
		actual, err := run.StringToLogLevel(testCase.input)
		if testCase.should_err && err == nil {
			t.Errorf("In test %d, TestStringToLogLevel(%v): expected error, actual %v",
				i+1, testCase.input, actual)
		}
		if err == nil && actual != testCase.expected {
			t.Errorf("In test %d, TestStringToLogLevel(%v): expected %v, actual %v",
				i+1, testCase.input, testCase.expected, actual)
		}
	}
}
