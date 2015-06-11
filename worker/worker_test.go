// Here be unit tests for the Worker.

package worker

import (
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/oryband/go-web-mapreduce/algorithm"
)

const (
	totalInputSize = 100
	inputSize      = 10
	reduceJobs     = 3
)

type WorkerSuite struct {
	suite.Suite
	algorithm *algorithm.Algorithm
	worker    Worker
}

func (s *WorkerSuite) SetupSuite() {
	JobTTL = 100 * time.Millisecond
}

func (s *WorkerSuite) SetupTest() {
	s.algorithm, _, _, _ = algorithm.NewMockAlgorithm(totalInputSize, inputSize, reduceJobs)
}

func (s *WorkerSuite) TearDownTest() {
	done := make(chan struct{})
	go func() {
		s.worker.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-s.worker.CloseMeChannel():
		s.Fail("worker asked to be closed after Close()")
	case <-time.After(2 * JobTTL):
		s.Fail("worker wasn't closed fast enough")
	}
}

// TestGetters test the simple getter functions of the worker interface.
func (s *WorkerSuite) TestGetters() {
	s.NotNil(s.worker.CloseMeChannel())
	s.NotNil(s.worker.ReceiveChannel())
	s.NotEmpty(s.worker.ID())
	s.NotEmpty(s.worker.String())
}

// TestAssignJob tests assigning a job to the worker in various scenarios.
func (s *WorkerSuite) TestAssignJob() {
	// Test no job is returned if no job has been assigned to the worker.
	s.Nil(s.worker.GetJob())

	// Test nil job.
	s.Panics(func() { s.worker.AssignJob(nil) })

	// Test assigning a proper job, then getting it.
	j, _ := s.algorithm.GetIncompleteJob()
	s.Require().NotPanics(func() { s.worker.AssignJob(j) })
	s.Equal(j, s.worker.GetJob())

	// Test assigning a job before completing the previous one causes a panic.
	s.Panics(func() { s.worker.AssignJob(j) })
}

// TestCompleteJob tests completing a worker's assigned job.
func (s *WorkerSuite) TestCompleteJob() {
	// Test completing a job when no job is assigned causes a panic.
	s.Panics(func() { s.worker.CompleteJob() })

	j, _ := s.algorithm.GetIncompleteJob()
	s.worker.AssignJob(j)
	s.NotPanics(func() { s.worker.CompleteJob() })
}

// TestCompleteJobTwice tests completing a job after already completing an
// assigned job. I.E. completing when no job is assigned.
//
// NOTE this test is not a part of TestCompleteJob() because we want to test
// that calling Close() on test teardown is completed properly in both cases
// where CompleteJob() is called properly and improperly.
func (s *WorkerSuite) TestCompleteJobTwice() {
	j, _ := s.algorithm.GetIncompleteJob()
	s.worker.AssignJob(j)
	s.worker.CompleteJob()
	s.Panics(func() { s.worker.CompleteJob() })
}

// TestJobTTL tests whether worker asks to be clsoed
// after its assigned job ttl has expired.
func (s *WorkerSuite) TestJobTTL() {
	j, _ := s.algorithm.GetIncompleteJob()
	s.worker.AssignJob(j)
	select {
	case <-s.worker.CloseMeChannel():
	case <-time.After(2 * JobTTL):
		s.Fail("worker didn't ask to be closed after job ttl has expired")
	}
}
