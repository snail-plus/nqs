package common

import log "github.com/sirupsen/logrus"

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
	log.Infof("stop %s", r.Name)
}

func (r DaemonTask) IsStopped() bool {
	return r.stopped
}
