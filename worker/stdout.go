package Worker

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/fizx/logs"
)

type StdOutWorker struct {
	WorkChannel chan map[string]interface{}
	QuitChannel chan bool
}

// Init does nothing
func (worker *StdOutWorker) Init() {

}

// Start the work
func (worker *StdOutWorker) Start() {
	logs.Debug("Worker is %v", worker)
	go worker.Work()
}

// Work the queue
func (worker *StdOutWorker) Work() {
	worker.startTime = time.Now()
	logs.Info("StdOutWorker starting work at %v", worker.startTime)
	for {
		select {
		case obj := <-worker.WorkChannel:
			logs.Debug("Worker received: %v", obj)
			line, err := json.Marshal(obj)
			if err != nil {
				logs.Info("Unable to marshal object %v", obj)
				break
			}
			fmt.Println(string(line))

		case <-worker.QuitChannel:
			logs.Info("Worker received quit")
			return
		}
	}
}

// Stop stops the worker by send a message on its quit channel
func (worker *StdOutWorker) Stop() {
	worker.QuitChannel <- true
}
