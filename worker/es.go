package worker

import (
	"fmt"
	"net/url"
	"time"

	"github.com/fizx/logs"
	"github.com/spf13/viper"
)

// ElasticSearchWorker bulk uploads to ElasticSearch
type ElasticSearchWorker struct {
	WorkChannel  chan map[string]interface{}
	QuitChannel  chan bool
	SendToStdOut bool
	endpoint     string
	counter      int
	totalCounter int64
	items        []string
	startTime    time.Time
	lastTime     time.Time
	lastCount    int64
}

func ConfiguredElasticSearchHost() string {
	key := "es.host"
	if viper.IsSet(key) {
		return viper.GetString(key)
	}
	return "localhost"
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

func ConfiguredElasticSearchMaxItems() int {
	key := "es.max"
	if viper.IsSet(key) {
		return viper.GetInt(key)
	}
	return 500
}

func (worker *ElasticSearchWorker) Init() (err error) {
	worker.endpoint = fmt.Sprintf("%s://%s:%d/_bulk", ConfiguredElasticSearchScheme(), ConfiguredElasticSearchHost(), ConfiguredElasticSearchPort())
	_, err = url.Parse(worker.endpoint)
	if err != nil {
		logs.Fatal("Invalid Elastic Search endpoint: %v", worker.endpoint)
		err = fmt.Errorf("Invalid Elastic Search endpoint: %v", worker.endpoint)
		return
	}
	worker.counter = 0
	worker.items = make([]string, ConfiguredElasticSearchMaxItems()*2) // need to make room for create commands
	return
}

/*
// Start the work
func (worker *ElasticSearchWorker) Start() {
	logs.Debug("Worker is %v", worker)
	logs.Debug("Worker config is %v:", worker.Config)
	worker.counter = 0
	worker.items = make([]string, worker.Config.Max*2) // need to make room for create commands
	if worker.Config != nil {
		worker.endpoint = fmt.Sprintf("http://%s:%d/_bulk", worker.Config.Host, worker.Config.Port)
		_, err := url.Parse(worker.endpoint)
		if err != nil {
			logs.Fatal("Invalid Elastic Search endpoint: %v", worker.endpoint)
		}
		logs.Info("Set Elastic Search endpoint to %v", worker.endpoint)
		go worker.Work()
	} else {
		logs.Fatal("No Elastic Search configuration given")
	}

}

// Work the queue
func (worker *ElasticSearchWorker) Work() {
	worker.startTime = time.Now()
	worker.lastTime = worker.startTime
	logs.Info("ElasticSearchWorker starting work at %v", worker.startTime)
	for {
		select {
		case obj := <-worker.WorkChannel:
			logs.Debug("Worker received: %v; current count is %v", obj, worker.counter)
			if worker.counter >= worker.Config.Max*2 {
				worker.flush(false)
			}
			line, err := json.Marshal(obj)
			if err != nil {
				logs.Info("Unable to marshal object %v", obj)
				break
			}
			createDoc := fmt.Sprintf(`{"create": { "_index": "%s", "_type": "%s"}}`,
				worker.Config.Index, worker.Config.DocumentType)
			worker.items[worker.counter] = createDoc
			worker.items[worker.counter+1] = string(line)
			worker.counter += 2

		case <-worker.QuitChannel:
			logs.Info("Worker received quit")
			return
		}
	}
}

// Stop stops the worker by send a message on its quit channel
func (worker *ElasticSearchWorker) Stop() {
	worker.QuitChannel <- true
	worker.flush(true)
}

func (worker *ElasticSearchWorker) flush(forceReport bool) {
	flushEvery := int64(worker.Config.InfoFlushEvery)
	worker.totalCounter++
	if worker.counter > 0 {
		if !worker.SendToStdOut {
			str := strings.Join(worker.items[0:worker.counter], "\n") + "\n"
			bs := []byte(str)
			req, _ := http.NewRequest("POST", worker.endpoint, bytes.NewBuffer(bs)) // endpoint has already been vetted
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
					logs.Debug("POST succeeded on flush %v", worker.totalCounter)
					logs.Debug("response Status: %v", resp.Status)
					body, _ := ioutil.ReadAll(resp.Body)
					logs.Debug("response Body: %v", string(body))
				} else {
					logs.Warn("On flush %v, Post failed with status: %v", worker.totalCounter, resp.StatusCode)
					logs.Warn("response Status: %v", resp.Status)
					body, _ := ioutil.ReadAll(resp.Body)
					logs.Warn("response Body: %v", string(body))
				}
				defer resp.Body.Close()
			}
			logs.Debug("Bulk upload is complete")
		} else { // test mode: send to standout
			str := strings.Join(worker.items[0:worker.counter], "\n") + "\n"
			fmt.Print(str)
		}
		itemCount := ((worker.totalCounter - 1) * int64(worker.Config.Max)) + int64(worker.counter/2)
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
			report.TotalElapsedTime = float64(now.Sub(worker.startTime)) / float64(time.Second)
			report.TimeSinceLastFlush = float64(now.Sub(worker.lastTime)) / float64(time.Second)
			worker.lastTime = now
			report.ItemsFlushed = itemCount - worker.lastCount
			worker.lastCount = itemCount
			report.ItemsPerSecond = float64(report.ItemsFlushed) / report.TimeSinceLastFlush
			strReport, _ := json.Marshal(report)
			logs.Info("%v", string(strReport))
		}
		// now, clear out state
		worker.items = make([]string, worker.Config.Max*2)
		worker.counter = 0
	}
}
*/
