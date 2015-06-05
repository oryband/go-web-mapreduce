package algorithm

import (
	"fmt"

	log "github.com/Sirupsen/logrus"

	"github.com/oryband/go-web-mapreduce/protocol"
)

// Job is a single map or reduce task, assigned to an active worker.Worker.
//
// NOTE it implements fmt.Stringer interface for logging purposes.
type Job struct {
	id             protocol.JobID
	input          protocol.Input
	partitionIndex protocol.PartitionIndex // Zero value (empty string) for all map jobs.
	code           protocol.AlgorithmCode
	isMapJob       bool
	isComplete     bool
}

// newJob returns a new Job.
func newJob(
	id protocol.JobID,
	input protocol.Input,
	partitionIndex protocol.PartitionIndex,
	code protocol.AlgorithmCode,
	isMapJob bool) *Job {

	// Sanity checks.
	switch {
	case input == nil:
		panic("received nil job input")
	case len(input) == 0 && isMapJob:
		// Empty input is only possible for reduce jobs.
		panic("received empty map job input")
	case isMapJob && partitionIndex != 0:
		panic("job received non-zero partition index with map job")
	case len(code) == 0:
		panic("received empty algorithm code")
	}

	// Empty reduce job input is possible, but suspicious.
	if len(input) == 0 && !isMapJob {
		log.WithFields(log.Fields{"job": id, "partition index": partitionIndex}).Warn("received empty reduce job input")
	}

	j := Job{
		id:         id,
		input:      input,
		code:       code,
		isMapJob:   isMapJob,
		isComplete: false,
	}

	if !isMapJob {
		j.partitionIndex = partitionIndex
	}

	return &j
}

// NewJobMeta returns a new job message's *protocol.Meta.
func (j *Job) NewJobMeta() *protocol.Meta {
	m := protocol.Meta{
		Type:           protocol.NewJob,
		JobID:          j.ID(),
		PartitionIndex: j.partitionIndex,
	}

	if j.IsMapJob() {
		m.JobType = protocol.MapJob
	} else {
		m.JobType = protocol.ReduceJob
	}

	return &m
}

// Input returns the job's protocol.Input.
func (j *Job) Input() protocol.Input { return j.input }

// ID returns the job's identifying ID.
func (j *Job) ID() protocol.JobID { return j.id }

// Code returns the job's identifying Code.
func (j *Job) Code() protocol.AlgorithmCode { return j.code }

// IsMapJob returns true if job is a mapper job. Always equals to !IsReduceJob().
func (j *Job) IsMapJob() bool { return j.isMapJob }

// IsReduceJob returns true if job is a reducer job. Always equals to !IsMapJob().
func (j *Job) IsReduceJob() bool { return !j.IsMapJob() }

// IsComplete returns true if job has been completed.
func (j *Job) IsComplete() bool { return j.isComplete }

// Complete marks the job as complete.
func (j *Job) Complete() {
	if j.isComplete {
		panic("trying to set Job.isComplete to true but is already true")
	}
	j.isComplete = true
}

// Returns job's string representation.
func (j *Job) String() string {
	var t protocol.JobType
	if j.IsMapJob() {
		t = protocol.MapJob
	} else {
		t = protocol.ReduceJob
	}
	return fmt.Sprintf("%s/%s", t.String(), j.ID())
}
