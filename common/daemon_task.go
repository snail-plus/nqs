package common

type DaemonTask struct {
	Name    string
	Run     func()
	stopped bool
}

func (r DaemonTask) Start() {
	go func() {
		r.Run()
	}()
}

func (r DaemonTask) Stop() {
	r.stopped = true
}

func (r DaemonTask) IsStopped() bool {
	return r.stopped
}
