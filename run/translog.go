package run

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/spf13/viper"
	"github.com/willf/translog/worker"

	"github.com/fizx/logs"
)

const configLogLevel = "logging.level"
const configTranslogOverWritePidFile = "pid.overwrite"
const configTranslogPidFile = "pid.file"
const configRuntimeCpus = "runtime.cpus"
const configLogFile = "logging.file"

// StringToLogLevel converts a string to a log leve
func StringToLogLevel(config string) (level logs.Level, err error) {
	uconfig := strings.ToUpper(config)
	if uconfig == "DEBUG" {
		level = logs.DEBUG
		return
	}
	if uconfig == "INFO" {
		level = logs.INFO
		return
	}
	if uconfig == "WARN" {
		level = logs.WARN
		return
	}
	if uconfig == "ERROR" {
		level = logs.ERROR
		return
	}
	if uconfig == "FATAL" {
		level = logs.FATAL
		return
	}
	err = fmt.Errorf("Invalid log level specification: %s", config)
	return
}

func setViperDefaults() {
	viper.SetDefault(configLogLevel, "INFO")
	viper.SetDefault(configTranslogOverWritePidFile, true)
	viper.SetDefault(configTranslogPidFile, "/var/translog.pid")
	viper.SetDefault(configRuntimeCpus, runtime.NumCPU())
}

func createPidFile(pidFileName string) (pidFile *os.File, err error) {
	_, err = os.Stat(pidFileName)
	if err == nil { // file exists
		if viper.GetBool(configTranslogOverWritePidFile) {
			os.Remove(pidFileName)
		} else {
			err = fmt.Errorf("PID file already exists: %s", pidFileName)
			logs.Warn(err)
			return
		}
	}
	pidFile, err = os.OpenFile(pidFileName, os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		logs.Warn("Unable to create PID file %s.", pidFileName)
		return
	}
	pid := os.Getpid()
	logs.Info("Writing pid %d to PID file %s.", pid, pidFileName)
	pidString := fmt.Sprintf("%d", pid)
	pidFile.Write([]byte(pidString))
	pidFile.Close()
	return
}

func Run(sink worker.Worker) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	if viper.IsSet(configLogFile) {
		logFile := viper.GetString(configLogFile)
		handle, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			logs.Warn("Unable to open log file %s, using stderr: %s", logFile, err)
		} else {
			logs.Logger = log.New(handle, "", log.Ldate|log.Ltime|log.Lmicroseconds)
		}
	}
	level, err := StringToLogLevel(viper.GetString(configLogLevel))
	if err == nil {
		logs.SetLevel(level)
	} else {
		logs.SetLevel(logs.DEBUG)
		logs.Warn("Invalid log level specification %s; setting to DEBUG", viper.GetString(configLogLevel))
	}
	logs.Info("Starting translog")

	pidFileName := viper.GetString(configTranslogPidFile)
	_, err = createPidFile(pidFileName)
	// set the number of Cpus
	runtime.GOMAXPROCS(viper.GetInt(configRuntimeCpus))

	// create the channels

	work := make(chan map[string]interface{})

	logWorker := &worker.LogParser{}

	logWorker.SetWorkChannel(work)
	logWorker.Init()

	sink.SetWorkChannel(work)
	sink.Init()

	go logWorker.Start()
	go sink.Start()

	sigs := make(chan os.Signal, 1)
	finished := make(chan bool, 0)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	fmt.Fprintf(os.Stderr, "Logging to %v. Send SIGINT or SIGTERM to %v to stop.\n", viper.GetString(configLogLevel), os.Getpid())

	go func() {
		sig := <-sigs
		logs.Info("Stopping: Caught signal: %s", sig)
		logs.Info("Removing file %s", pidFileName)
		_, err = os.Stat(pidFileName)
		if err == nil {
			os.Remove(pidFileName)

		} else {
			logs.Warn("PID file %s did not exist.", pidFileName)
		}
		logWorker.Stop()
		sink.Stop()
		logs.Info("Exiting logs2es")
		finished <- true
	}()

	<-finished
}
