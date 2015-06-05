package algorithm

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/suite"

	"github.com/oryband/go-web-mapreduce/protocol"
	"github.com/oryband/go-web-mapreduce/test"
)

const (
	totalInputSize = 10001
	inputSize      = 102
	reduceJobs     = 10

	mockMapOutputSize    = 1001
	mockReduceOutputSize = 500

	logLevel = log.ErrorLevel
)

var mapJobs = int(math.Ceil(float64(totalInputSize) / float64(inputSize)))

type AlgorithmSuite struct {
	suite.Suite
	a *Algorithm
}

func TestAlgorithmSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(AlgorithmSuite))
}

// SetupSuite tests creating a new *Algorithm with various arguments,
// and checks the returned *Algorithm has all its initial values set properly.
func (s *AlgorithmSuite) SetupSuite() {
	log.SetLevel(logLevel)

	a, mapCode, reduceCode, input := NewMockAlgorithm(totalInputSize, inputSize, reduceJobs)
	a.Close()
	a = nil

	// Panic checks.
	for _, f := range []func(){
		func() { NewAlgorithm(protocol.AlgorithmCode(""), reduceCode, 1, 1, input) },
		func() { NewAlgorithm(mapCode, protocol.AlgorithmCode(""), 1, 1, input) },
		func() { NewAlgorithm(mapCode, reduceCode, 0, 1, input) },
		func() { NewAlgorithm(mapCode, reduceCode, 1, 0, input) },
		func() { NewAlgorithm(mapCode, reduceCode, 1, 1, nil) },
		func() { NewAlgorithm(mapCode, reduceCode, 1, 1, protocol.Input{}) },
	} {
		s.Require().Panics(f)
	}

	// Create a fresh new proper algorithm.
	// This is to make sure the previous panic calls didn't mingle with the used arguments.
	a, mapCode, reduceCode, input = NewMockAlgorithm(totalInputSize, inputSize, reduceJobs)
	defer a.Close()

	s.Require().NotNil(a)

	s.Require().Len(a.unassignedMapJobs, mapJobs)
	s.Require().Len(a.assignedMapJobs, 0)

	s.Require().Len(a.unassignedReduceJobs, 0)
	s.Require().Len(a.assignedReduceJobs, 0)
	s.Require().Len(a.unsetReduceJobs, reduceJobs)
	s.Require().Len(a.partitions, reduceJobs)

	s.Require().Equal(mapCode, a.mapCode)
	s.Require().Equal(reduceCode, a.reduceCode)

	select {
	case _, open := <-a.CompletedChannel():
		s.Require().False(open)
	default:
	}

	s.Require().False(a.IsMapJobsComplete())
	s.Require().False(a.IsReduceJobsComplete())
	s.Require().False(a.IsComplete())
}

// SetupTest creates a new master with a mocked algorithm.
func (s *AlgorithmSuite) SetupTest() {
	a, _, _, _ := NewMockAlgorithm(totalInputSize, inputSize, reduceJobs)
	s.a = a
}

func (s *AlgorithmSuite) TeardownTest() {
	s.a.Close()
	select {
	case _, open := <-s.a.done:
		s.Require().False(open)
	default:
	}
}

// TestCompleteMapJob creates an algorithm and completes all map jobs with mocked job output.
// It then tests whether the job's output was handled correctly,
// and if sent as input to related pending reduce job partitions.
func (s *AlgorithmSuite) TestCompleteMapJob() {
	// Test that the job output was copied to the algorithm's reduce partitions.
	// NOTE we're creating a new copy of the mock map output,
	// since it presumably could've been changed when sent to CompleteMapJob().

	for i := 0; i < mapJobs; i++ {
		j := testGetIncompleteJob(s, i)
		s.Require().NotPanics(func() {
			s.a.CompleteMapJob(j.ID(), test.MockMapJobOutput(totalInputSize, mockMapOutputSize, []int{0, 1, 3, 4}))
		})
		s.Condition(func() bool { _, ok := s.a.assignedMapJobs[j.ID()]; return !ok })

		// No reason this job should be inserted to reduce jobs queue,
		// as a NEW reduce job should be created using this job's output as input.
		// But I'm checking this anyways.
		s.Condition(func() bool { _, ok := s.a.assignedReduceJobs[j.ID()]; return !ok })
	}

	// Check algorithm partitions were updated correctly on every completed map job.
	partitions := make([][]protocol.Input, reduceJobs)
	for i := range partitions {
		partitions[i] = make([]protocol.Input, 0)
	}

	for i := 0; i < mapJobs; i++ {
		for index, partition := range test.MockMapJobOutput(totalInputSize, mockMapOutputSize, []int{0, 1, 3, 4}) {
			partitions[index] = append(partitions[index], partition)
		}
	}
	s.Require().Equal(partitions, s.a.partitions)

	// Wait until reduce phase has started i.e. all reduce jobs have been initialized.
	done := make(chan struct{})
	go func() {
		// Ticking is necessary because otherwise this is a goroutine-starving infinite loop.
		for range time.Tick(TrackPhasesInterval) {
			if len(s.a.unsetReduceJobs) == 0 {
				close(done)
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.Tick(3 * TrackPhasesInterval):
		s.Require().Fail("map phase didn't change to reduce phase fast enough after all map jobs have completed")
	}

	// Test "algorithm completed" channel is still open, i.e. algorithm is not complete
	select {
	case _, open := <-s.a.CompletedChannel():
		if !open {
			s.Fail("completed channel is closed")
		}
	default:
	}
}

// TestCompleteReduceJob is similar to TestCompleteMapJob,
// but tests completing reduce job using mocked partition inputs.
func (s *AlgorithmSuite) TestCompleteReduceJob() {
	// Get and complete all map jobs.
	for i := 0; i < mapJobs; i++ {
		// Complete a job with the mocked output.
		j := testGetIncompleteJob(s, i)
		s.a.CompleteMapJob(j.ID(), test.MockMapJobOutput(totalInputSize, mockMapOutputSize, []int{0, 1, 3, 4}))
	}

	s.Require().True(s.a.IsMapJobsComplete())
	s.Require().Len(s.a.assignedMapJobs, 0)
	s.Require().Len(s.a.unassignedMapJobs, 0)
	s.Require().False(s.a.IsReduceJobsComplete())
	s.Require().False(s.a.IsComplete())

	// Wait for reduce phase to start.
	done := make(chan struct{})
	go func() {
		for range time.Tick(TrackPhasesInterval) {
			if len(s.a.unsetReduceJobs) == 0 {
				close(done)
				return
			}
		}
	}()
	<-done

	// Test all reduce jobs have been initialized.
	s.Require().Len(s.a.unsetReduceJobs, 0)

	// Get and complete all reduce jobs.
	for i := 0; i < reduceJobs; i++ {
		j := testGetIncompleteJob(s, 0)
		s.Require().NotPanics(func() { s.a.CompleteReduceJob(j.ID(), test.MockReduceJobOutput(mockReduceOutputSize)) })
		s.Condition(func() bool { _, ok := s.a.assignedReduceJobs[j.ID()]; return !ok })
		// No reason this job should be inserted to map jobs queue, but I'm checking this anyways.
		s.Condition(func() bool { _, ok := s.a.assignedMapJobs[j.ID()]; return !ok })
	}

	s.True(s.a.IsComplete())
	s.Len(s.a.assignedMapJobs, 0)
	s.Len(s.a.unassignedMapJobs, 0)
	s.Len(s.a.assignedReduceJobs, 0)
	s.Len(s.a.unassignedReduceJobs, 0)
	s.Len(s.a.unsetReduceJobs, 0)

	time.Sleep(TrackPhasesInterval)
	select {
	case _, open := <-s.a.CompletedChannel():
		s.False(open)
	default:
		s.Fail("completed channel is open")
	}
}

// TestCancelJobPanics tests calling CancelJob() with invalid arguments
// is causing panics.
func (s *AlgorithmSuite) TestCancelJobPanics() {
	// Panic checks.
	s.Panics(func() { s.a.CancelJob(nil) })
	s.Panics(func() { s.a.CancelJob(<-s.a.unassignedMapJobs) })
}

// TestCancelJob tests cancelling incomplete job can be re-fetched afterwards,
// i.e. not lost and never returned to the unassigned job queues.
func (s *AlgorithmSuite) TestCancelJob() {
	// Map phase: Get all map jobs, and cancel every even job. Complete all odd jobs.
	for i := 0; i < mapJobs; i++ {
		j := testGetIncompleteJob(s, i)
		if i%2 == 0 {
			s.Require().NotPanics(func() { s.a.CancelJob(j) })
		} else {
			s.a.CompleteMapJob(j.ID(), test.MockMapJobOutput(totalInputSize, mockMapOutputSize, []int{0, 1, 3, 4}))
		}
	}

	s.Require().False(s.a.IsMapJobsComplete())
	s.Require().Len(s.a.assignedMapJobs, 0)
	s.Require().Len(s.a.unassignedMapJobs, int(math.Ceil(float64(mapJobs)/2.0)))

	// Fetch and complete all canceled jobs.
	for i := 0; i < mapJobs; i++ {
		if i%2 == 0 {
			j := testGetIncompleteJob(s, i)
			s.a.CompleteMapJob(j.ID(), test.MockMapJobOutput(totalInputSize, mockMapOutputSize, []int{0, 1, 3, 4}))
		}
	}

	s.Require().True(s.a.IsMapJobsComplete())
	s.Require().Len(s.a.assignedMapJobs, 0)
	s.Require().Len(s.a.unassignedMapJobs, 0)

	// Wait for reduce phase to start.
	done := make(chan struct{})
	go func() {
		for range time.Tick(TrackPhasesInterval) {
			if len(s.a.unsetReduceJobs) == 0 {
				close(done)
				return
			}
		}
	}()
	<-done

	// Reduce phase: Repeat the same test.
	for i := 0; i < reduceJobs; i++ {
		j := testGetIncompleteJob(s, i)
		if i%2 == 0 {
			s.Require().NotPanics(func() { s.a.CancelJob(j) })
		} else {
			s.a.CompleteReduceJob(j.ID(), test.MockReduceJobOutput(mockReduceOutputSize))
		}
	}

	s.Require().False(s.a.IsReduceJobsComplete())
	s.Require().Len(s.a.assignedReduceJobs, 0)
	s.Require().Len(s.a.unassignedReduceJobs, int(math.Ceil(float64(reduceJobs)/2.0)))

	for i := 0; i < reduceJobs; i++ {
		if i%2 == 0 {
			j := testGetIncompleteJob(s, i)
			s.a.CompleteReduceJob(j.ID(), test.MockReduceJobOutput(mockReduceOutputSize))
		}
	}

	s.Require().True(s.a.IsComplete())
	s.Require().Len(s.a.assignedReduceJobs, 0)
	s.Require().Len(s.a.unassignedReduceJobs, 0)

	time.Sleep(TrackPhasesInterval)
	select {
	case _, open := <-s.a.CompletedChannel():
		s.False(open)
	default:
		s.Fail("completed channel is open")
	}
}

// TestConcurrentJobs tests a full algorithm life time with concurrent job fetching and completion.
func (s *AlgorithmSuite) TestConcurrentJobs() {
	rand.Seed(time.Now().UnixNano())

	// Get and complete jobs concurrently.
	jobs := make(chan *Job, mapJobs+reduceJobs)
	canceled := int64(0)
	wg := sync.WaitGroup{}
	wg.Add(300)
	for i := 0; i < 300; i++ {
		go func() {
			defer wg.Done()

			for {
				select {
				case <-s.a.CompletedChannel():
					return
				case j := <-jobs:
					// Have a 20% probabilty to cancel the job, else complete it.
					if rand.Intn(100) < 20 {
						s.a.CancelJob(j)
						atomic.AddInt64(&canceled, 1)
						continue
					}

					if j.IsMapJob() {
						s.a.CompleteMapJob(j.ID(), test.MockMapJobOutput(totalInputSize, mockMapOutputSize, []int{0, 1, 3, 4}))
					} else { // Reduce job.
						s.a.CompleteReduceJob(j.ID(), test.MockReduceJobOutput(mockReduceOutputSize))
					}
				}
			}
		}()
	}

	// Get incomplete jobs concurrently.
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer wg.Done()

			for {
				// Necessary because of "default" case, otherwise goroutine won't yield.
				runtime.Gosched()

				select {
				case <-s.a.CompletedChannel():
					return
				default:
					j, err := s.a.GetIncompleteJob()
					switch {
					case err != nil:
					case j != nil:
						jobs <- j
					}
				}
			}
		}()
	}

	wg.Wait()
	s.T().Logf("canceled: %d/%d", canceled, mapJobs+reduceJobs)
}

// testGetIncompleteJob is a helper function that tests GetIncompleteJob() return an incomplete map job.
// It also checks that an errors is returned if all jobs have been fetched i.e. assigned to workers,
// or if algorithm has been completed.
func testGetIncompleteJob(s *AlgorithmSuite, i int) *Job {
	switch {
	case !s.a.IsMapJobsComplete():
		s.Require().True(i < mapJobs, test.CallingFunctionInfo(2))
	case !s.a.IsReduceJobsComplete():
		s.Require().True(i < reduceJobs, test.CallingFunctionInfo(2))
	default:
		s.Require().True(s.a.IsComplete(), test.CallingFunctionInfo(2))
	}

	j, err := s.a.GetIncompleteJob()
	if err != nil {
		s.Require().True(s.a.IsComplete())
	}
	s.Require().False(s.a.IsComplete(), test.CallingFunctionInfo(2))
	s.Require().NotNil(j, test.CallingFunctionInfo(2))

	switch {
	case !s.a.IsMapJobsComplete():
		if i == mapJobs {
			// All map jobs have been fetched.
			s.Require().Fail(fmt.Sprintf("GetIncompleteJob() returned a map job instead of blocking after all have been assigned, %s", test.CallingFunctionInfo(2)))
		}
		s.Require().True(j.IsMapJob(), test.CallingFunctionInfo(2))
		s.Require().Equal(s.a.mapCode, j.Code(), test.CallingFunctionInfo(2))
		return j
	case !s.a.IsReduceJobsComplete():
		if i == reduceJobs {
			// All reduce jobs have been fetched.
			s.Require().Fail(fmt.Sprintf(
				"GetIncompleteJob() returned a reduce job instead of blocking after all have been assigned, %s", test.CallingFunctionInfo(2)))
		}
		s.Require().True(j.IsReduceJob(), test.CallingFunctionInfo(2))
		s.Require().Equal(s.a.reduceCode, j.Code(), test.CallingFunctionInfo(2))
		return j
	default:
		return nil
	}
}
