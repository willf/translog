package worker_test

import (
	"bytes"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/willf/translog/worker"
)

func TestTesting(t *testing.T) {
	if true != !false {
		t.Errorf("True is not false")
	}
}

var logConfig = []byte(`
[parse]
    pattern = (?P<host>\S+) (?P<client>\S+) (?P<user>\S+) \[(?P<time>[^\]]+)\] "((?P<method>[A-Z]+) )?(?P<uri>\S+).*"
  `)

var URITestCases = []struct {
	input    string
	expected interface{}
	found    bool // expected error
}{
	{"info", int64(7), true},
	{"friendly", "giant", true},
	{"unknown", nil, false},
	{"bool", false, true},
	{"float", 5.4, true},
	{"string", "testing", true},
	{"test", true, true},
	{"_test", "found", true},
	{"count", int64(8), true},
}

func TestParseURI(t *testing.T) {
	// viper.SetConfigType("toml")
	viper.ReadConfig(bytes.NewBuffer(logConfig))
	uri := "/0/gif?info=7&friendly=giant&bool=false&float=5.4&string=testing&test=found&unused=100&count=8"
	m := make(map[string]interface{})
	m["test"] = true
	w := &worker.LogParser{}
	w.Init()
	w.ParseURI(uri, m)
	for i, tt := range URITestCases {
		actual, found := m[tt.input]
		if actual != tt.expected || found != tt.found {
			t.Errorf("In test %d, TestParseURI(%v): expected %v, actual %v; type of expected: %v, type of actual: %v; expected found: %v, actual: %v",
				i+1, tt.input, tt.expected, actual, reflect.TypeOf(tt.expected), reflect.TypeOf(actual), tt.found, found)
		}
	}
}

func TestParseEvents(t *testing.T) {
	viper.SetConfigType("toml") // or viper.SetConfigType("YAML")
	viper.ReadConfig(bytes.NewBuffer(logConfig))

	line := `0.155.42.201 - - [26/Jul/2011:00:00:04 +0000] "GET /0.gif?server_ms=231&loadtime=2739&testfloat=6.4&testbool=false&timediff=-4&locale=en-us&referrer=http%3A%2F%2Fwww.archive.org%2Fsearch.php%3Fquery%3D%2528collection%253AFurthur%2520OR%2520mediatype%253AFurthur%2529%2520AND%2520-mediatype%253Acollection%26sort%3Ddate%26page%3D11&version=2&count=7 HTTP/1.1" 200 35 "http://www.archive.org/details/furthur2011-07-23.akg568.goldberg.114824.flac16" "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; GTB6.6; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.30729; .NET CLR 3.5.30729; .NET4.0C)"`
	w := &worker.LogParser{}
	w.Init()
	m, err := w.ParseEvents(line)
	if err != nil {
		t.Errorf("Couldn't parse example line.")
	}
	good := make(map[string]interface{})
	good["client"] = "-"
	good["host"] = "0.155.42.201"
	good["loadtime"] = int64(2739)
	good["locale"] = "en-us"
	good["referrer"] = "http://www.archive.org/search.php?query=%28collection%3AFurthur%20OR%20mediatype%3AFurthur%29%20AND%20-mediatype%3Acollection&sort=date&page=11"
	good["server_ms"] = int64(231)
	//good["created"] = time.Unix(int64(1311638404), 0).UTC()
	good["timediff"] = int64(-4)
	good["user"] = "-"
	good["version"] = int64(2)
	good["testfloat"] = float64(6.4)
	good["testbool"] = false

	for key, kv := range good {
		if m[key] != kv {
			t.Errorf("Testing for %v; expected: %v, actual: %v; expected type: %v; actual type: %v",
				key, kv, m[key], reflect.TypeOf(kv), reflect.TypeOf(m[key]))
		}
	}
	ti := time.Unix(int64(1311638404), 0).UTC()
	ti2 := m["created"].(time.Time)
	if !ti.Equal(ti2) {
		t.Errorf("Time was not parsed correctly")
	}
}

func TestParseTimeStrings(t *testing.T) {
	expected := time.Unix(int64(1311638400), 0).UTC()
	inputs := []string{
		"26/Jul/2011:00:00:00 +0000",
		"Tue Jul 26 00:00:00 2011",
		"Tue Jul 26 00:00:00 UTC 2011",
		"Tue Jul 26 00:00:00 +0000 2011",
		"26 Jul 11 00:00 UTC",
		"26 Jul 11 00:00 +0000",
		"Tuesday, 26-Jul-11 00:00:00 UTC",
		"Tue, 26 Jul 2011 00:00:00 UTC",
		"Tue, 26 Jul 2011 00:00:00 +0000",
		"2011-07-26T00:00:00Z",
		"2011-07-26T00:00:00.0Z"}
	for i, input := range inputs {
		actual := worker.ParseStringForValue(input).(time.Time)
		if !actual.Equal(expected) {
			t.Errorf("Parse time string failure, test %v: expected: %v, actual: %v", i+1, actual, expected)
		}
	}
}

type IntFormatTest struct {
	input    string
	expected int64
}

func TestParseIntStrings(t *testing.T) {
	var formatTests = []IntFormatTest{
		{"0", 0},
		{"1", 1},
		{"-1", -1},
		{"12123", 12123},
		{"+12123", 12123},
		{"-12123", -12123},
	}
	for i, test := range formatTests {
		actual := worker.ParseStringForValue(test.input)
		if actual != test.expected {
			t.Errorf("test parse int failure: %v expected %v got %v from %v", i+1, test.expected, actual, test.input)
		}
	}
}

type DoubleFormatTest struct {
	input    string
	expected float64
}

func floatEquals(a, b float64) bool {
	var EPSILON float64 = 0.0001
	diff := math.Abs(a - b)
	if diff <= EPSILON {
		return true
	}
	return false
}

func TestParseDoubleStrings(t *testing.T) {
	var formatTests = []DoubleFormatTest{
		{"0.0", 0.0},
		{"1.0", 1.0},
		{"-1.0", -1.0},
		{"12123.0", 12123.0},
		{"+12123.0", 12123.0},
		{"-12123.0", -12123.0},
		{"2.71828182845904523536028747", math.E},
		{"1.618033988749894848204586834365638", math.Phi},
	}
	for i, test := range formatTests {
		actual := worker.ParseStringForValue(test.input)
		if !floatEquals(actual.(float64), test.expected) {
			t.Errorf("test parse float failure: %v expected %f [type: %v] got %f [type: %v] from %v",
				i+1, test.expected, reflect.TypeOf(test.expected), actual, reflect.TypeOf(actual), test.input)
		}
	}
}

type BoolFormatTest struct {
	input    string
	expected bool
}

func TestParseBool(t *testing.T) {
	var formatTests = []BoolFormatTest{
		{"true", true},
		{"false", false},
		{"TRUE", true},
		{"FALSE", false},
		{"True", true},
		{"False", false},
	}
	for i, test := range formatTests {
		actual := worker.ParseStringForValue(test.input)
		if actual != test.expected {
			t.Errorf("test parse bool failure: %v expected %v got %v from %v", i+1, test.expected, actual, test.input)
		}
	}
}
