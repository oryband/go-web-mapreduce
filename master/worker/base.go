package worker

import (
	"net/http"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/satori/go.uuid"

	"github.com/oryband/go-web-mapreduce/master/algorithm"
	"github.com/oryband/go-web-mapreduce/master/protocol"
)

// BaseWorker implements the Worker interface, but without network communication.
// It is embedded in the WebWorker, and is also used in unit tests.
type BaseWorker struct {
	id protocol.WorkerID // Worker's identifying ID.

	Job           *algorithm.Job         // Currently assigned job.  nil if unassigned.
	Send, Receive chan *protocol.Message // send/receive message channels. Messages.Received are handled by the algorithm.

	JobComplete chan struct{}  // Initialized on every new job assignment and closed when job has been completed.
	CloseMe     chan struct{}  // Worker can close this channel in order to ask the master to close the worker.
	Done        chan struct{}  // Closed when worker closes. Used to stop worker's incomplete job TTL goroutine.
	closing     sync.WaitGroup // Used to block Close() untill all goroutines exit.
}

// NewBaseWorker returns a new mock worker.
// It has the same signature as NewWorker,
// and is public so it can be used in unit tests.
//
// See Master.newWorkerFunc() for further info.
func NewBaseWorker(_ http.ResponseWriter, _ *http.Request) (Worker, error) {
	w := BaseWorker{
		id:      protocol.WorkerID(uuid.NewV4()),
		Send:    make(chan *protocol.Message, SendChanSize),
		Receive: make(chan *protocol.Message, ReceiveChanSize),
		CloseMe: make(chan struct{}, CloseMeChanSize),
		Done:    make(chan struct{}),
	}

	return &w, nil
}

func (w *BaseWorker) String() string                           { return w.id.String() }
func (w *BaseWorker) ID() protocol.WorkerID                    { return w.id }
func (w *BaseWorker) ReceiveChannel() <-chan *protocol.Message { return w.Receive }
func (w *BaseWorker) CloseMeChannel() <-chan struct{}          { return w.CloseMe }
func (w *BaseWorker) GetJob() *algorithm.Job                   { return w.Job }

func (w *BaseWorker) AssignJob(j *algorithm.Job) {
	log.WithFields(log.Fields{"worker": w, "job": j}).Info("assigning job to worker")
	defer log.WithFields(log.Fields{"worker": w, "job": j}).Info("job successfully assigned to worker")

	// Sanity checks.
	switch {
	case j == nil:
		panic("worker received nil job")
	case w.Job != nil && !w.Job.IsComplete():
		panic("assigning new job while worker is already assigned an incomplete job")
	case w.JobComplete != nil:
		panic("assigning new job whie worker's job complete channel is non-nil")
	}

	w.Job = j

	// Kill worker after ttl has expired. TTL will be cancelled by the master
	// once the job has been completed or canceled by the master.
	w.JobComplete = make(chan struct{})
	w.closing.Add(1)
	go func(completed chan struct{}) {
		defer w.closing.Done()

		select {
		case <-w.Done:
		case <-completed:
		case <-time.After(JobTTL):
			w.CloseMe <- struct{}{}
		}
	}(w.JobComplete)

	w.Send <- protocol.NewMessage(j.Input(), j.Code(), j.NewJobMeta())
}

func (w *BaseWorker) CompleteJob() {
	log.WithFields(log.Fields{"worker": w, "job": w.Job}).Info("stopping worker's job ttl")
	defer log.WithField("worker", w).Info("stopped worker's job ttl")

	if w.JobComplete == nil {
		panic("job complete is nil")
	}

	close(w.JobComplete)
	w.JobComplete = nil
}

// Close closes job expiration TTL goroutine.
func (w *BaseWorker) Close() {
	log.WithField("worker", w).Info("closing base worker")
	defer log.WithField("worker", w).Info("base worker successfully closed")

	close(w.Done)
	w.closing.Wait()
}
