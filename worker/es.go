package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/fizx/logs"
	"github.com/spf13/viper"
)

// ElasticSearchWorker bulk uploads to ElasticSearch
type ElasticSearchWorker struct {
	WorkChannel  chan map[string]interface{}
	QuitChannel  chan bool
	robinIndex   int
	robinLock    sync.Mutex
	counter      int
	totalCounter int64
	items        []string
	startTime    time.Time
	lastTime     time.Time
	lastCount    int64
	max          int
	randInter    *rand.Rand
	hosts        []string
	port         int
	scheme       string
	index        string
	documentType string
	reportEvery  int64
	mocking      bool
	useSuffix    bool
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

func ConfiguredElasticSearchReportEvery() int64 {
	key := "es.report_every"
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
	w.QuitChannel = make(chan bool)

	w.counter = 0
	w.robinIndex = 0
	w.max = ConfiguredElasticSearchMax()
	w.hosts = ConfiguredElasticSearchHosts()
	w.scheme = ConfiguredElasticSearchScheme()
	w.port = ConfiguredElasticSearchPort()
	w.index = ConfiguredElasticSearchIndex()
	w.documentType = ConfiguredElasticSearchDocumentType()
	w.reportEvery = ConfiguredElasticSearchReportEvery()
	w.mocking = ConfiguredElasticSearchMocking()
	w.useSuffix = ConfiguredElasticSearchUseDateSuffix()
	_, err = url.Parse(w.Endpoint())
	if err != nil {
		logs.Fatal("Invalid Elastic Search endpoint: %v", w.Endpoint())
		err = fmt.Errorf("Invalid Elastic Search endpoint: %v", w.Endpoint())
		return
	}
	w.items = make([]string, w.max*2) // need to make room for create commands
	return
}

func (w *ElasticSearchWorker) NextHost() string {
	hosts := w.hosts
	if len(hosts) == 0 {
		return "localhost"
	}
	i := rand.Int31n(int32(len(hosts)))
	return w.hosts[i]
}

func (w *ElasticSearchWorker) Endpoint() string {
	host := w.NextHost()
	endpoint := fmt.Sprintf("%s://%s:%d/_bulk", w.scheme, host, w.port)
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
	return w.documentType
}

func (w *ElasticSearchWorker) ReportEvery() int64 {
	return w.reportEvery
}

func (w *ElasticSearchWorker) Mocking() bool {
	return w.mocking
}

func (w *ElasticSearchWorker) UseDateSuffix() bool {
	return w.useSuffix
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
			if w.counter >= w.max*2 || w.Mocking() {
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
			logs.Info("Elasticsearch worker received quit")
			return
		}
	}
}

// Stop stops the w by send a message on its quit channel
func (w *ElasticSearchWorker) Stop() {
	w.QuitChannel <- true
	w.flush(true)
}

//"took": 68,
//  "errors": false,
//  "items": [
//    {
//      "create": {
//        "_index": "es-prod-analytics-2016.04.01",
//        "_type": "event",
//        "_id": "AVPTILBJZqnc5MImoVd8",
//        "_version": 1,
//        "status": 201
//      }
//    },

func itemsCreated(body []byte) []string {
	var idre = regexp.MustCompile(`\"_id\":\"(?P<id>[^\"]*)\"`)
	matches := idre.FindAllStringSubmatch(string(body), -1)
	var ids []string
	ids = make([]string, len(matches))
	for i, id := range matches {
		ids[i] = id[1]
	}
	return ids
}

func (w *ElasticSearchWorker) flush(forceReport bool) {
	reportEvery := w.ReportEvery()
	w.totalCounter++
	if w.counter > 0 {
		var body []byte
		if !w.Mocking() {
			str := strings.Join(w.items[0:w.counter], "\n") + "\n"
			go func() {
				bs := []byte(str)
				req, err := http.NewRequest("POST", w.Endpoint(), bytes.NewBuffer(bs))
				if err != nil {
					// should not happend...
					logs.Warn("Creating request resulted in an error: %v", err)
				}
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
						logs.Info("POST succeeded with status %v on flush %v", resp.Status, w.totalCounter)
						body, _ = ioutil.ReadAll(resp.Body)
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
			}()
		} else { // test mode: send to standout
			str := strings.Join(w.items[0:w.counter], "\n") + "\n"
			fmt.Print(str)
		}
		itemCount := ((w.totalCounter - 1) * int64(w.max)) + int64(w.counter/2)
		if forceReport || (reportEvery > 0 && itemCount%reportEvery == 0) {
			go func() {
				now := time.Now()
				var report struct {
					ItemCount          int64   `json:"item_count,omitempty"`
					TotalElapsedTime   float64 `json:"total_elapsed_time,omitempty"`
					TimeSinceLastFlush float64 `json:"time_since_last_flush,omitempty"`
					ItemsFlushed       int64   `json:"items_flushed,omitempty"`
					ItemsPerSecond     float64 `json:"items_per_second,omitempty"`
					LastItemCreated    string  `json:"last_item_created,omitempty"`
				}
				report.ItemCount = itemCount
				report.TotalElapsedTime = float64(now.Sub(w.startTime)) / float64(time.Second)
				report.TimeSinceLastFlush = float64(now.Sub(w.lastTime)) / float64(time.Second)
				w.lastTime = now
				report.ItemsFlushed = itemCount - w.lastCount
				w.lastCount = itemCount
				report.ItemsPerSecond = float64(report.ItemsFlushed) / report.TimeSinceLastFlush
				itemsCreated := itemsCreated(body)
				if len(itemsCreated) > 0 {
					report.LastItemCreated = itemsCreated[len(itemsCreated)-1]
				}
				strReport, _ := json.Marshal(report)
				logs.Info("%v", string(strReport))
			}()
		}
		// now, clear out state
		_ = w.Init()
	}
}
