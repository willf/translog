package worker

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/fizx/logs"
)

type StdOutWorker struct {
	WorkChannel chan map[string]interface{}
	QuitChannel chan bool
	startTime   time.Time
}

func (w *StdOutWorker) SetWorkChannel(channel chan map[string]interface{}) {
	w.WorkChannel = channel
}

//
func (w *StdOutWorker) Init() (err error) {
	w.QuitChannel = make(chan bool)
	return
}

// Start the work
func (w *StdOutWorker) Start() {
	logs.Debug("Worker is %v", w)
	go w.Work()
}

// Work the queue
func (w *StdOutWorker) Work() {
	w.startTime = time.Now()
	logs.Info("StdOutWorker starting work at %v", w.startTime)
	for {
		select {
		case obj := <-w.WorkChannel:
			logs.Debug("Worker received: %v", obj)
			line, err := json.Marshal(obj)
			if err != nil {
				logs.Info("Unable to marshal object %v", obj)
				break
			}
			fmt.Println(string(line))

		case <-w.QuitChannel:
			logs.Info("Worker received quit")
			return
		}
	}
}

// Stop stops the worker by send a message on its quit channel
func (w *StdOutWorker) Stop() {
	w.QuitChannel <- true
}
