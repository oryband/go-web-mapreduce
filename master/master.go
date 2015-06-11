// Package master implements a Web MapReduce master, managing remote MapReduce workers.
package master

import (
	"fmt"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/satori/go.uuid"

	"github.com/oryband/go-web-mapreduce/algorithm"
	"github.com/oryband/go-web-mapreduce/protocol"
	"github.com/oryband/go-web-mapreduce/worker"
)

// Master manages a single MapReduce algorithm task.
// It distributes the algorithm's map and reduce jobs,
// and links between map and reduce workers.
//
// NOTE it implements fmt.Stringer interface for logging purposes.
type Master struct {
	id uuid.UUID

	algorithm *algorithm.Algorithm

	sync.RWMutex
	unassigned chan worker.Worker // Workers available for a job assignment.

	done    chan struct{}  // Closed when master shutdowns, and used to stop master goroutines.
	closing sync.WaitGroup // Used to block Close() method until goroutines exit.
}

// New receives algorithm arguments and returns a new master.
func New(
	maxWorkers int,
	mapCode, reduceCode protocol.AlgorithmCode,
	inputLen, numPartitions uint64,
	totalInput protocol.Input) *Master {

	return &Master{
		id:         uuid.NewV4(),
		algorithm:  algorithm.NewAlgorithm(mapCode, reduceCode, inputLen, numPartitions, totalInput),
		unassigned: make(chan worker.Worker, maxWorkers),
		done:       make(chan struct{}),
	}
}

// Start makes the master start assigning workers and handle jobs.
func (m *Master) Start() {
	m.closing.Add(1)
	go m.assignJobs() // Repeatedly assign incomplete, unassigned jobs to unassigned workers.
}

// Close stops assigning new jobs to workers and shuts them down.
func (m *Master) Close() {
	log.Info("closing master")
	defer log.Info("master closed")

	// Notify all worker pumps to close their workers,
	// and close global job-assignment goroutine.
	close(m.done)
	m.closing.Wait()
}

// AddWorker initializes a new worker using given worker.Initializer.
// The incoming client can be any type that implements this interface.
//
// It pushes the new worker to the unassigned workers queue,
// and listens for incoming messages.
func (m *Master) AddWorker(init worker.Initializer) error {
	m.closing.Add(1)
	defer m.closing.Done()

	log.Info("adding worker")
	defer log.Info("finished adding worker")

	w, err := init.NewWorker()
	if err != nil {
		log.Error(err)
		return err
	}

	// Repeatedly read and process messages received from the worker.
	m.closing.Add(1)
	go m.workerPump(w)

	m.unassigned <- w

	return nil
}

// assignJobs repeatedly assigns incomplete, unassigned jobs to unassigned workers.
func (m *Master) assignJobs() {
	defer m.closing.Done()

	log.Info("starting to assign jobs")
	defer log.Info("stopping to assign new jobs")

	for {
		select {
		case <-m.done:
			return
		case <-m.CompletedChannel():
			return
		case t := <-m.unassigned:
			// Get a new unassigned incomplete job.
			j, err := m.algorithm.GetIncompleteJob()
			if err != nil {
				// Releast worker back to a unassigned queue
				// if no incomplete job was available and algorithm is still incomplete.
				if m.algorithm.IsComplete() {
					return
				}

				m.unassigned <- t
				continue
			}

			t.AssignJob(j)
		}
	}
}

// workerPump repeatedly reads and processes messages received from the worker.
// It shutdowns the worker and cancels its job on any error.
func (m *Master) workerPump(w worker.Worker) {
	defer m.closing.Done()

	log.WithField("worker", w).Info("starting worker pump")
	defer log.WithField("worker", w).Info("stopping worker pump")

	defer m.closeWorker(w)
	for {
		select {
		case <-m.done:
			return
		case <-w.CloseMeChannel():
			log.WithField("worker", w).Warn("worker asked to be closed")
			return
		case msg := <-w.ReceiveChannel():
			log.WithFields(log.Fields{"worker": w, "job": w.GetJob(), "message": msg}).Info("received message from worker")
			switch msg.Meta.Type {
			case protocol.JobComplete:
				if err := m.processJobCompleteMessage(w, msg); err != nil {
					log.WithFields(log.Fields{"worker": w, "job": w.GetJob()}).Error(err)
					return
				}
			default:
				log.WithFields(log.Fields{"worker": w, "job": w.GetJob(), "message": msg}).
					Error(fmt.Errorf("unexpected message received from worker"))
				return
			}
		}
	}
}

// processJobCompleteMessage marks worker's job as complete and assigns it a new job.
func (m *Master) processJobCompleteMessage(w worker.Worker, msg *protocol.Message) error {
	// Stop worker's job expiration TTL.
	w.CompleteJob()

	// Sanity checks.
	j := w.GetJob()
	switch {
	case len(msg.Input) > 0:
		return fmt.Errorf("message received has non-empty input")
	case j.ID() != msg.Meta.JobID:
		return fmt.Errorf("message received from worker has different job id than the job assigned to it")
	case j.IsMapJob() && msg.Meta.JobType != protocol.MapJob:
		return fmt.Errorf("message received from worker doesn't have map job type")
	case j.IsReduceJob() && msg.Meta.JobType != protocol.ReduceJob:
		return fmt.Errorf("message received from worker doesn't have map job type")
	case j.IsMapJob() && len(msg.MapOutput) == 0:
		// Empty job output is possible, but suspicious. Thus, we log a warning.
		log.WithFields(log.Fields{"worker": w, "job": j, "mesasge": msg}).Warn("map job output is empty")
	case j.IsReduceJob() && len(msg.ReduceOutput) == 0:
		log.WithFields(log.Fields{"worker": w, "job": j, "mesasge": msg}).Warn("reduce job output is empty")
	}

	// Mark job as complete if not already completed by another worker.
	switch {
	case j.IsComplete():
		log.WithFields(log.Fields{"worker": w, "job": j, "message": msg}).
			Warn("ignoring complete message because job has already completed")
	case j.IsMapJob():
		m.algorithm.CompleteMapJob(j.ID(), msg.MapOutput)
	case j.IsReduceJob():
		m.algorithm.CompleteReduceJob(j.ID(), msg.ReduceOutput)
	}

	// Release worker for a new assignment.
	m.unassigned <- w

	return nil
}

// closeWorker cancels the worker's job if assigned, and shuts it down.
func (m *Master) closeWorker(w worker.Worker) {
	log.WithField("worker", w).Info("closing worker")
	defer log.WithField("worker", w).Info("worker closed")

	// Cancel worker's job if assigned.
	if j := w.GetJob(); j != nil && !j.IsComplete() {
		m.algorithm.CancelJob(j)
	}

	// Close worker connection.
	w.Close()
}

// ID returns the master's identifying ID.
func (m *Master) ID() uuid.UUID  { return m.id }
func (m *Master) String() string { return m.ID().String() }

// CompletedChannel returns the master's algorithm completion nofitication channel.
func (m *Master) CompletedChannel() <-chan struct{} { return m.algorithm.CompletedChannel() }

// Results return the algorithm's results list.
//
// NOTE this might be nil if called before algorithm is complete.
//
// TODO add tests.
func (m *Master) Results() protocol.Input { return m.algorithm.Results() }
