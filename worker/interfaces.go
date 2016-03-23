package worker

// A Worker defines a standard interface for Initializing, Starting, and Stopping
// work
type Worker interface {
	Init()
	Start()
	Stop()
}
