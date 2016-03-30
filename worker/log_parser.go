package worker

/*
	log_parser.go parse logs

	It especially defines a LogWorker type, that can read lines of the log,
	and convert them into JSON format. It shares a channel with the
	with which to communicate the JSON objects it finds.

	It uses the tail library to read the log.

	The worker configuation information is found in config.go.
*/
import (
	"fmt"
	"math"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ActiveState/tail"
	"github.com/fizx/logs"
	"github.com/spf13/viper"
)

const configParseInputFile = "parse.input_file"
const configParseKeysToIgnore = "parse.keys_to_ignore"
const configParsePattern = "parse.pattern"
const configParseTimePatterns = "parse.time_patterns"
const configTailFromBeginning = "tail.from_beginning"
const configTailReopen = "tail.reopen"

// DefaultParseLogPattern is the default pattern for understanding log patterns
const DefaultParseLogPattern = `(?P<line>.*)` // `(?P<host>\S+) (?P<client>\S+) (?P<user>\S+) \[(?P<created>[^\]]+)\] "((?P<method>[A-Z]+) )?(?P<uri>\S+).*"`

// LogParser parses the imput and puts events on a channel
type LogParser struct {
	Channel chan map[string]interface{}
	tailer  *tail.Tail
	Regex   *regexp.Regexp
	pattern string
	lock    sync.Mutex
}

func newKeyName(k string, m map[string]interface{}) string {
	name := k
	_, found := m[name]
	if !found {
		return name
	}
	return newKeyName("_"+k, m)
}

func sliceContains(list []string, a string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func (w *LogParser) shouldIgnore(key string) bool {
	keysToIgnore := viper.GetStringSlice(configParseKeysToIgnore)
	return key == "" || sliceContains(keysToIgnore, key)
}

func ParseStringForValue(ts string) interface{} {
	timePatterns := viper.GetStringSlice(configParseTimePatterns)
	for _, timePattern := range timePatterns {
		t, e := time.Parse(timePattern, ts)
		if e == nil {
			return t
		}
	}
	nginxFormat := "02/Jan/2006:15:04:05 -0700"
	t, e := time.Parse(nginxFormat, ts)
	if e == nil {
		return t
	}
	t, e = time.Parse(time.ANSIC, ts)
	if e == nil {
		return t
	}
	t, e = time.Parse(time.UnixDate, ts)
	if e == nil {
		return t
	}
	t, e = time.Parse(time.RubyDate, ts)
	if e == nil {
		return t
	}
	t, e = time.Parse(time.RFC822, ts)
	if e == nil {
		return t
	}
	t, e = time.Parse(time.RFC822Z, ts)
	if e == nil {
		return t
	}
	t, e = time.Parse(time.RFC850, ts)
	if e == nil {
		return t
	}
	t, e = time.Parse(time.RFC1123, ts)
	if e == nil {
		return t
	}
	t, e = time.Parse(time.RFC1123Z, ts)
	if e == nil {
		return t
	}
	t, e = time.Parse(time.RFC3339, ts)
	if e == nil {
		return t
	}
	t, e = time.Parse(time.RFC3339Nano, ts)
	if e == nil {
		return t
	}
	pi, err := strconv.ParseInt(ts, 10, 64)
	if err == nil {
		return pi
	}
	pb, err := strconv.ParseBool(ts)
	if err == nil {
		return pb
	}
	pf, err := strconv.ParseFloat(ts, 64)
	if err == nil {
		// it might be the string "NaN"
		if !math.IsNaN(pf) {
			return pf
		}
		return 0.0 // return 0.0 for NaN
	}
	return ts
}

// ParseURI parses the URI string and adds the relevant query parameters
// into the main map.
// it also attempts to determine the data type of the items by
// parsing as date, int, bool, float, and if all of these fail, then keeping
// as string
func (w *LogParser) ParseURI(uri string, v map[string]interface{}) {
	if uri != "" {
		url, err := url.Parse(uri)
		if err == nil {
			q := url.Query()
			for k, kvs := range q {
				newKey := newKeyName(k, v)
				if !w.shouldIgnore(newKey) && len(kvs) > 0 {
					v[newKey] = ParseStringForValue(kvs[0])
				}
			}
		}
	}
}

// ParseEvents parses the line (including a call to ParseURI) to
// add events to the map of strings -> anything. It returns that map
func (w *LogParser) ParseEvents(line string) (map[string]interface{}, error) {
	v := make(map[string]interface{})
	regex := w.CachedRegex()
	match := regex.FindStringSubmatch(line)
	names := regex.SubexpNames()
	if match != nil {
		for i, submatch := range match {
			name := names[i]
			if !w.shouldIgnore(name) {
				v[names[i]] = ParseStringForValue(submatch)
			}
			if name == "uri" {
				w.ParseURI(submatch, v)
			}
		}
		return v, nil
	}
	logs.Debug("Line %s did not match pattern.", line)
	return nil, fmt.Errorf("Line %s did not match pattern.", line)
}

// converts w config into tail Config
func (w *LogParser) convertConfig() (config tail.Config) {
	if !viper.GetBool(configTailFromBeginning) {
		config.Location = &tail.SeekInfo{0, os.SEEK_END}
	}
	config.ReOpen = viper.GetBool(configTailReopen)
	config.Follow = true
	config.Logger = tail.DiscardingLogger
	logs.Info("tail config: %v", config)
	return
}

func (w *LogParser) SetWorkChannel(channel chan map[string]interface{}) {
	w.Channel = channel
}

// recompile regex if necessaary ...

func (w *LogParser) CachedRegex() *regexp.Regexp {
	w.lock.Lock()
	pattern := viper.GetString(configParsePattern)
	if pattern != w.pattern {
		if pattern == "" {
			pattern = DefaultParseLogPattern
		}
		regex, err := regexp.Compile(pattern)
		if err != nil {
			logs.Warn("Could not compile Regex. Error: %v", err)
		} else {
			logs.Debug("Resetting regex: %v", pattern)
			w.pattern = pattern
			w.Regex = regex
		}
	}
	w.lock.Unlock()
	return w.Regex
}

// Init initializes worker's Regex
func (w *LogParser) Init() {
	w.CachedRegex()
}

// Start starts the LogWorker.
// it starts tailing the log file, and parsing lines from it
// putting parsed lines on the shared channel.
func (w *LogParser) Start() {
	logs.Info("Starting LOG PARSING process")
	w.Init()

	inputFile := viper.GetString(configParseInputFile)
	t, err := tail.TailFile(inputFile,
		w.convertConfig())
	if err != nil {
		logs.Warn("Input file could not be opened: %s; error: %s", inputFile, err)

	} else {
		w.tailer = t
		for line := range t.Lines {
			s := strings.TrimSpace(line.Text)
			logs.Debug("Processing line %v", s)
			v, err := w.ParseEvents(s)
			if err == nil {
				go func() {
					w.Channel <- v
				}()
			}
		}
	}
	logs.Info("Stopping worker process")
}

// Stop stops the worker and cleans up. Does *not* stop ElasticSearchWorker
func (w *LogParser) Stop() {
	if w.tailer != nil {
		//logs.Debug("Stopping tailer")
		//err := w.tailer.Stop()
		logs.Debug("Cleaning up tailer")
		w.tailer.Cleanup()
		logs.Debug("Done stopping tailer")
	}
}
