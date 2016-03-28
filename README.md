# translog
A log file tailer that sends structured data to various backends

## Basic introduction

The typical use case is that a _log file_ is being appended to by some process,
likely being [rotated](https://en.wikipedia.org/wiki/Log_rotation) every so
often. The log lines themselves are can be parsed using some regular expression
with variables, and the data is recognizable as dates, strings, integers, booleans,
floats, or (URIs)[https://en.wikipedia.org/wiki/Uniform_Resource_Identifier].
Furthermore, if a URI is recognized, the query parameters of the URI are also
interpretable as dates, strings, integers, booleans, or floats.

For example, the following log line:
```
2016-04-01T11:00:00Z 8.8.8.8.8 http://entish.org?q=Bob&age=47
```
Could be recognized with the following regular expression:
```
(?P<created>\S+)\s+(?P<ip>\S+)\s+(?P<uri>*.)
```
And would result in the following structured information:
```
created: 2016-04-01T11:00:00Z
ip: "8.8.8.8"
uri: "http://entish.org?q=Bob&age=47"
q: "Bob"
age: 47
```

This structured format is then used by one of the configured sub-programs for
processing (sending to ElasticSearch, printing to stdout, etc).

## Configuration

Translog uses a a configuration file for many of its configuration files. It
can be in TOML, YAML, or JSON format (anything that [Viper](https://github.com/spf13/viper))
supports.

The following shows the default configuration values, including those which
are required, and have no default value:

```TOML
[pid]
file = "/var/translog.pid"   # where to store PID file
overwrite = true             # should the PID file be overwritten if it already exists

[logging]
file = "/var/translog.log"   # location of Log file (this is _Translog_'s log file)
level = "INFO"               # logging level (DEBUG/INFO/WARN/ERROR/FATAL)

[parse]
pattern = '(?P<line>.*)'        # structured patter.
input_file = "/tmp/example.log" # required; no default
time_patterns = []              # additional time patterns in [Golang time format](https://golang.org/pkg/time/#pkg-constants)
keys_to_ignore = []             # keys to *not* use in output


[cpus]
cpus = 4                     # defaults to the number of CPUs of machine

[tail]
from_beginning = false       # start processing log at end
reopen = true                # reopen files (like `tail -F`)

# ElasticSearch processing
[es]
mocking = false              # set to true to send to STDOUT
host = "localhost"           # ElasticSearch host
port = 9200                  # ElasticSearch port
scheme = "http"              # ElasticSearch scheme (http or https)
max = 500                    # how many documents to bulk-upload at a time
flush_every = 10000          # how many documents to process before bulk uploading
index = "analytics"          # name of index
document_type = "event"      # name of document type
use_date_suffix = false      # add YYYY.MM.DD to end of document type

# File processing
[file]
out = "output.jsonl"          # file name to write JSON objects to
```
