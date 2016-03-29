package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/fizx/logs"
	"github.com/spf13/viper"
)

// ElasticSearchWorker bulk uploads to ElasticSearch
type ElasticSearchWorker struct {
	WorkChannel   chan map[string]interface{}
	QuitChannel   chan bool
	robinIndex    int
	robinLock     sync.Mutex
	counter       int
	totalCounter  int64
	items         []string
	startTime     time.Time
	lastTime      time.Time
	lastCount     int64
	index         string
	docType       string
	flushEvery    int64
	mocking       bool
	useDateSuffix bool
}

func ConfiguredElasticSearchHosts() []string {
	key := "es.hosts"
	if viper.IsSet(key) {
		return viper.GetStringSlice(key)
	}
	hosts := make([]string, 1)
	hosts[0] = "localhost"
	return hosts
}

func ConfiguredElasticSearchPort() int {
	key := "es.port"
	if viper.IsSet(key) {
		return viper.GetInt(key)
	}
	return 9200
}

func ConfiguredElasticSearchScheme() string {
	key := "es.scheme"
	if viper.IsSet(key) {
		return viper.GetString(key)
	}
	return "http"
}

func ConfiguredElasticSearchMax() int {
	key := "es.max"
	if viper.IsSet(key) {
		return viper.GetInt(key)
	}
	return 500
}

func ConfiguredElasticSearchIndex() string {
	key := "es.index"
	if viper.IsSet(key) {
		return viper.GetString(key)
	}
	return "analytics"
}

func ConfiguredElasticSearchDocumentType() string {
	key := "es.document_type"
	if viper.IsSet(key) {
		return viper.GetString(key)
	}
	return "event"
}

func ConfiguredElasticSearchFlushEvery() int64 {
	key := "es.flush_every"
	if viper.IsSet(key) {
		return int64(viper.GetInt(key))
	}
	return int64(10000)
}

func ConfiguredElasticSearchMocking() bool {
	key := "es.mocking"
	if viper.IsSet(key) {
		return viper.GetBool(key)
	}
	return false
}

func ConfiguredElasticSearchUseDateSuffix() bool {
	key := "es.use_date_suffix"
	if viper.IsSet(key) {
		return viper.GetBool(key)
	}
	return false
}

func (w *ElasticSearchWorker) Init() (err error) {

	_, err = url.Parse(w.Endpoint())
	if err != nil {
		logs.Fatal("Invalid Elastic Search endpoint: %v", w.Endpoint())
		err = fmt.Errorf("Invalid Elastic Search endpoint: %v", w.Endpoint())
		return
	}
	w.counter = 0
	w.robinIndex = 0
	w.items = make([]string, ConfiguredElasticSearchMax()*2) // need to make room for create commands
	w.index = ConfiguredElasticSearchIndex()
	w.docType = ConfiguredElasticSearchDocumentType()
	w.flushEvery = ConfiguredElasticSearchFlushEvery()
	w.mocking = ConfiguredElasticSearchMocking()
	w.useDateSuffix = ConfiguredElasticSearchUseDateSuffix()
	return
}

func (w *ElasticSearchWorker) NextHost() string {
	hosts := ConfiguredElasticSearchHosts()
	w.robinLock.Lock()
	host := hosts[w.robinIndex]
	w.robinIndex = (w.robinIndex + 1) % len(hosts)
	w.robinLock.Unlock()
	return host
}

func (w *ElasticSearchWorker) Endpoint() string {
	host := w.NextHost()
	endpoint := fmt.Sprintf("%s://%s:%d/_bulk", ConfiguredElasticSearchScheme(), host, ConfiguredElasticSearchPort())
	return endpoint
}

func (w *ElasticSearchWorker) CurrentCount() int {
	return w.counter
}

func (w *ElasticSearchWorker) CurrentItems() []string {
	return w.items
}

func (w *ElasticSearchWorker) Index() string {
	return w.index
}

func (w *ElasticSearchWorker) DocumentType() string {
	return w.docType
}

func (w *ElasticSearchWorker) FlushEvery() int64 {
	return w.flushEvery
}

func (w *ElasticSearchWorker) Mocking() bool {
	return w.mocking
}

func (w *ElasticSearchWorker) UseDateSuffix() bool {
	return w.useDateSuffix
}

func (w *ElasticSearchWorker) SetWorkChannel(channel chan map[string]interface{}) {
	w.WorkChannel = channel
}

// Start the work
func (w *ElasticSearchWorker) Start() {
	go w.Work()
}

// Work the queue
func (w *ElasticSearchWorker) Work() {
	w.startTime = time.Now()
	w.lastTime = w.startTime
	logs.Info("ElasticSearchWorker starting work at %v", w.startTime)
	for {
		select {
		case obj := <-w.WorkChannel:
			logs.Debug("worker received: %v; current count is %v", obj, w.counter)
			if w.counter >= ConfiguredElasticSearchMax()*2 || w.Mocking() {
				w.flush(false)
			}
			line, err := json.Marshal(obj)
			if err != nil {
				logs.Info("Unable to marshal object %v", obj)
				break
			}
			docType := w.DocumentType()
			index := w.Index()
			if w.UseDateSuffix() {
				index += time.Now().Format("2006.01.02")
			}
			createDoc := fmt.Sprintf(`{"create": { "_index": "%s", "_type": "%s"}}`,
				index, docType)
			w.items[w.counter] = createDoc
			w.items[w.counter+1] = string(line)
			w.counter += 2

		case <-w.QuitChannel:
			logs.Info("w received quit")
			return
		}
	}
}

// Stop stops the w by send a message on its quit channel
func (w *ElasticSearchWorker) Stop() {
	w.QuitChannel <- true
	w.flush(true)
}

func (w *ElasticSearchWorker) flush(forceReport bool) {
	flushEvery := w.FlushEvery()
	w.totalCounter++
	if w.counter > 0 {
		if !w.Mocking() {
			str := strings.Join(w.items[0:w.counter], "\n") + "\n"
			bs := []byte(str)
			req, _ := http.NewRequest("POST", w.Endpoint(), bytes.NewBuffer(bs)) // endpoint has already been vetted
			req.Header.Set("Content-Type", "application/json")
			logs.Debug("--START BULK DATA--")
			logs.Debug("%s", string(bs))
			logs.Debug("--END BULK DATA--")

			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				logs.Warn("POST failed: %s", err)
			} else {
				if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
					logs.Debug("POST succeeded on flush %v", w.totalCounter)
					logs.Debug("response Status: %v", resp.Status)
					body, _ := ioutil.ReadAll(resp.Body)
					logs.Debug("response Body: %v", string(body))
				} else {
					logs.Warn("On flush %v, Post failed with status: %v", w.totalCounter, resp.StatusCode)
					logs.Warn("response Status: %v", resp.Status)
					body, _ := ioutil.ReadAll(resp.Body)
					logs.Warn("response Body: %v", string(body))
				}
				defer resp.Body.Close()
			}
			logs.Debug("Bulk upload is complete")
		} else { // test mode: send to standout
			str := strings.Join(w.items[0:w.counter], "\n") + "\n"
			fmt.Print(str)
		}
		itemCount := ((w.totalCounter - 1) * int64(ConfiguredElasticSearchMax())) + int64(w.counter/2)
		if forceReport || (flushEvery > 0 && itemCount%flushEvery == 0) {
			now := time.Now()
			var report struct {
				ItemCount          int64   `json:"item_count,omitempty"`
				TotalElapsedTime   float64 `json:"total_elapsed_time,omitempty"`
				TimeSinceLastFlush float64 `json:"time_since_last_flush,omitempty"`
				ItemsFlushed       int64   `json:"items_flushed,omitempty"`
				ItemsPerSecond     float64 `json:"items_per_second,omitempty"`
			}
			report.ItemCount = itemCount
			report.TotalElapsedTime = float64(now.Sub(w.startTime)) / float64(time.Second)
			report.TimeSinceLastFlush = float64(now.Sub(w.lastTime)) / float64(time.Second)
			w.lastTime = now
			report.ItemsFlushed = itemCount - w.lastCount
			w.lastCount = itemCount
			report.ItemsPerSecond = float64(report.ItemsFlushed) / report.TimeSinceLastFlush
			strReport, _ := json.Marshal(report)
			logs.Info("%v", string(strReport))
		}
		// now, clear out state
		_ = w.Init()
	}
}
