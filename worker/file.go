package worker

import (
	"encoding/json"
	"os"
	"time"

	"github.com/fizx/logs"
	"github.com/spf13/viper"
)

type FileWorker struct {
	WorkChannel chan map[string]interface{}
	QuitChannel chan bool
	startTime   time.Time
	out         *os.File
}

func (w *FileWorker) SetWorkChannel(channel chan map[string]interface{}) {
	w.WorkChannel = channel
}

func ConfiguredFileOutputName() string {
	key := "file.output"
	if viper.IsSet(key) {
		return viper.GetString(key)
	}
	return "output.jsonl"
}

//
func (w *FileWorker) Init() (err error) {
	w.QuitChannel = make(chan bool)
	fileName := ConfiguredFileOutputName()
	handle, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		logs.Warn("Unable to create output file %s because of %s", fileName, err)
	} else {
		w.out = handle
	}
	return
}

// Start the work
func (w *FileWorker) Start() {
	logs.Debug("Worker is %v", w)
	go w.Work()
}

// Work the queue
func (w *FileWorker) Work() {
	w.startTime = time.Now()
	logs.Info("FileWorker starting work at %v", w.startTime)
	for {
		select {
		case obj := <-w.WorkChannel:
			logs.Debug("Worker received: %v", obj)
			line, err := json.Marshal(obj)
			if err != nil {
				logs.Info("Unable to marshal object %v", obj)
				break
			}
			w.out.WriteString(string(line))
			w.out.WriteString("\n")

		case <-w.QuitChannel:
			logs.Info("Worker received quit")
			return
		}
	}
}

// Stop stops the worker by send a message on its quit channel
func (w *FileWorker) Stop() {
	w.out.Close()
	w.QuitChannel <- true
}
