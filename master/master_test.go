package master

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/oryband/go-web-mapreduce/algorithm"
	"github.com/oryband/go-web-mapreduce/protocol"
	"github.com/oryband/go-web-mapreduce/test"
	"github.com/oryband/go-web-mapreduce/worker"
)

const (
	totalInputSize = 20001
	inputSize      = 102
	reduceJobs     = 10

	mockMapOutputSize    = 1001
	mockReduceOutputSize = 500

	concurrentWorkers = 500

	logLevel = log.FatalLevel
)

var mapJobs = int(math.Ceil(float64(totalInputSize) / float64(inputSize)))

// TestBadWorkerMessages mocks sending wrong messages to the master from a worker,
// and tests whether that worker is closed.
func TestBadWorkerMessages(t *testing.T) {
	log.SetLevel(logLevel)

	t.Parallel()

	// Make sure worker isn't closed due to job TTL expiring.
	worker.JobTTL = 1 * time.Minute

	// We'll create multiple masters in this test.
	newMaster := func() (*Master, *worker.BaseWorker) {
		a, _, _, _ := algorithm.NewMockAlgorithm(totalInputSize, inputSize, reduceJobs)

		m := Master{
			algorithm:  a,
			unassigned: make(chan worker.Worker, concurrentWorkers),
			done:       make(chan struct{}),
		}

		assert.NoError(t, m.AddWorker(new(worker.BaseInitializer)))
		w := (<-m.unassigned).(*worker.BaseWorker)
		m.unassigned <- w

		return &m, w
	}

	m, w := newMaster()
	m.Start()
	<-w.Send
	w.Receive <- &protocol.Message{
		Meta: &protocol.Meta{
			Type:    protocol.JobComplete,
			JobID:   w.Job.ID(),
			JobType: protocol.ReduceJob,
		},
		MapOutput: test.MockMapJobOutput(totalInputSize, mockMapOutputSize, []int{0, 2, 4}),
	}

	select {
	case <-w.Done:
	case <-time.After(1 * time.Second):
		assert.Fail(t, "worker wasn't closed fast enough after sending bad message to master")
	}
	m.Close()

	// The following two messages should not close the worker, but print warnings to the log.
	m, w = newMaster()
	m.Start()
	<-w.Send
	w.Receive <- &protocol.Message{
		Meta: &protocol.Meta{
			Type:    protocol.JobComplete,
			JobID:   w.Job.ID(),
			JobType: protocol.MapJob,
		},
		MapOutput: make(map[protocol.PartitionIndex]protocol.Input),
	}

	select {
	case <-w.Done:
		assert.Fail(t, "worker was closed even though it sent a good message")
	case <-time.After(1 * time.Second):
	}
	m.Close()

	m, w = newMaster()
	m.Start()
	<-w.Send
	w.Receive <- &protocol.Message{
		Meta: &protocol.Meta{
			Type:    protocol.JobComplete,
			JobID:   w.Job.ID(),
			JobType: protocol.MapJob,
		},
		MapOutput: make(map[protocol.PartitionIndex]protocol.Input),
	}

	select {
	case <-w.Done:
		assert.Fail(t, "worker was closed even though it sent a good message")
	case <-time.After(1 * time.Second):
	}
	m.Close()
}

type MasterSuite struct {
	suite.Suite
	master              *Master
	mapCode, reduceCode protocol.AlgorithmCode
	input               protocol.Input
}

// TestMasterSuite executes the master's test suite.
func TestMasterSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(MasterSuite))
}

func (s *MasterSuite) SetupSuite() {
	log.SetLevel(logLevel)
}

// SetupTest creates a new master with a mocked algorithm.
func (s *MasterSuite) SetupTest() {
	a, mapCode, reduceCode, input := algorithm.NewMockAlgorithm(totalInputSize, inputSize, reduceJobs)

	s.mapCode = mapCode
	s.reduceCode = reduceCode
	s.input = input

	s.master = &Master{
		algorithm:  a,
		unassigned: make(chan worker.Worker, concurrentWorkers),
		done:       make(chan struct{}),
	}

	s.Require().Len(s.master.unassigned, 0)
}

// TearDownTest checks if the master is closed, and closes it if not.
func (s *MasterSuite) TearDownTest() {
	// Close master if not already closed.
	select {
	case <-s.master.done:
		s.Fail("master isn't closed")
	default:
		// Test master doesn't hang when closing.
		done := make(chan struct{})
		go func() {
			s.NotPanics(func() { s.master.Close() }, fmt.Sprintf("panic when closing master, %s", test.CallingFunctionInfo(2)))
			close(done)
		}()
		select {
		case <-done:
		case <-time.Tick(5 * time.Second):
			s.Require().Fail("master took too long to close")
		}
	}
}

// TestSerialAssignAndCompleteJobs creates a worker and test jobs are fetched and assigned to it.
func (s *MasterSuite) TestSerialAssignAndCompleteJobs() {
	if testing.Short() {
		s.T().Skip("short mode")
	}

	// Add worker.
	s.NoError(s.master.AddWorker(new(worker.BaseInitializer)))

	// Fetch the worker object from the unassigned queue and put it back there again.
	// We do this in order to test with that worker.
	var ww worker.Worker
	select {
	case ww = <-s.master.unassigned:
	case <-time.After(1 * time.Second):
		s.Require().Fail("new worker wasn't sent to unassigned queue fast enough")
	}
	w := ww.(*worker.BaseWorker)
	s.master.unassigned <- w

	// Start job assignment and worker pump goroutines.
	worker.JobTTL = 20 * time.Second
	s.master.Start()

	// Mock new job and job completed messages to and from the worker.
	// This should fetch and complete all map jobs.
	for i := 0; i < mapJobs; i++ {
		select {
		case <-w.Done:
			s.Require().Fail("worker was closed before completing all jobs")
		default:
		}

		msg := <-w.Send
		cmp := protocol.Message{
			Meta: &protocol.Meta{
				Type:    protocol.NewJob,
				JobID:   w.Job.ID(),
				JobType: protocol.MapJob,
			},
			Input: s.input[i*inputSize : int(math.Min(float64(1+totalInputSize), float64((i+1)*inputSize)))],
			Code:  s.mapCode,
		}
		if !s.Equal(&cmp, msg) {
			return
		}

		w.Receive <- &protocol.Message{
			Meta: &protocol.Meta{
				Type:    protocol.JobComplete,
				JobID:   w.Job.ID(),
				JobType: protocol.MapJob,
			},
			MapOutput: test.MockMapJobOutput(totalInputSize, mockMapOutputSize, []int{0, 2, 4}),
		}
	}

	// Wait until reduce phase has started i.e. all reduce jobs have been initialized.
	done := make(chan struct{})
	go func() {
		// Ticking is necessary because otherwise this is a goroutine-starving infinite loop.
		for range time.Tick(1 * time.Millisecond) {
			if s.master.algorithm.IsMapJobsComplete() {
				close(done)
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.Tick(50 * time.Millisecond):
		s.Require().Fail("map phase didn't change to reduce phase fast enough after all map jobs have completed")
	}

	// Repeat the process for reduce jobs:

	// Fill reduce inputs to compare against.
	partitions := make([]protocol.Input, reduceJobs)
	for index := range partitions {
		partitions[index] = make(protocol.Input, 0)
	}

	for i := 0; i < mapJobs; i++ {
		for index, partition := range test.MockMapJobOutput(totalInputSize, mockMapOutputSize, []int{0, 2, 4}) {
			var merged protocol.Input
			for _, v := range partition {
				merged = append(merged, v)
			}

			j, _ := strconv.ParseInt(string(index), 10, 64)
			partitions[j] = append(partitions[j], merged...)
		}
	}

	for i := 0; i < reduceJobs; i++ {
		select {
		case <-w.Done:
			s.Fail("worker was closed before completing all jobs")
			return
		default:
		}

		msg := <-w.Send
		cmp := protocol.Message{
			Meta: &protocol.Meta{
				Type:           protocol.NewJob,
				JobID:          w.Job.ID(),
				JobType:        protocol.ReduceJob,
				PartitionIndex: protocol.PartitionIndex(i),
			},
			Input: partitions[i],
			Code:  s.reduceCode,
		}
		s.Require().Equal(&cmp, msg)

		w.Receive <- &protocol.Message{
			Meta: &protocol.Meta{
				Type:           protocol.JobComplete,
				JobID:          w.Job.ID(),
				JobType:        protocol.ReduceJob,
				PartitionIndex: protocol.PartitionIndex(i),
			},
			ReduceOutput: test.MockReduceJobOutput(mockReduceOutputSize),
		}
	}

	time.Sleep(1 * time.Second)
	select {
	case _, open := <-s.master.CompletedChannel():
		s.False(open)
	default:
		s.Fail("completed channel is open")
	}
}

// TestConcurrentAssignAndCompleteJobs creates multiple concurrent workers
// and tests jobs are fetched, assigned, and completed concurrently.
func (s *MasterSuite) TestConcurrentAssignAndCompleteJobs() {
	if testing.Short() {
		s.T().Skip("short mode")
	}

	worker.JobTTL = 3 * time.Second

	// Add multiple workers.
	wg := sync.WaitGroup{}
	wg.Add(concurrentWorkers)
	for i := 0; i < concurrentWorkers; i++ {
		go func() {
			defer wg.Done()
			s.Require().NoError(s.master.AddWorker(new(worker.BaseInitializer)))
		}()
	}
	wg.Wait()

	// For each worker, repeatedly receive new jobs and complete them,
	// until algorithm is complete.
	s.Require().Len(s.master.unassigned, concurrentWorkers)
	wg.Add(concurrentWorkers)
	for i := 0; i < concurrentWorkers; i++ {
		go func(j int) {
			defer wg.Done()

			w := (<-s.master.unassigned).(*worker.BaseWorker)
			s.master.unassigned <- w

			// Make errors by making some workers not complete their job on purpose.
			for {
				// Have a third of the workers "die" and not respond,
				// and test the worker is closed after its TTL expires.
				if j%3 == 0 {
					return
				}

				select {
				case <-s.master.CompletedChannel():
					return
				case <-w.Done:
					return
				case msg := <-w.Send:
					// Mock time taken to complete job, then return results to master.
					time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

					if msg.Meta.JobType == protocol.MapJob {
						w.Receive <- &protocol.Message{
							Meta: &protocol.Meta{
								Type:    protocol.JobComplete,
								JobID:   w.Job.ID(),
								JobType: protocol.MapJob,
							},
							MapOutput: test.MockMapJobOutput(totalInputSize, mockMapOutputSize, []int{0, 2, 4}),
						}
					} else { // Reduce job.
						w.Receive <- &protocol.Message{
							Meta: &protocol.Meta{
								Type:           protocol.JobComplete,
								JobID:          w.Job.ID(),
								JobType:        protocol.ReduceJob,
								PartitionIndex: msg.Meta.PartitionIndex,
							},
							ReduceOutput: test.MockReduceJobOutput(mockReduceOutputSize),
						}
					}
				}
			}
		}(i)
	}

	// Start assigning jobs and wait for algorithm to complete.
	s.master.Start()
	wg.Wait()

	select {
	case <-s.master.CompletedChannel():
	case <-time.After(2 * time.Second):
		s.Fail("all workers have finished but algorithm isn't complete yet")
	}
}
