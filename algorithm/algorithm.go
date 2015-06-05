// Package algorithm provides thread-safe MapReduce algorithm management types and functions.
package algorithm

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/satori/go.uuid"

	"github.com/oryband/go-web-mapreduce/protocol"
)

// Time interval between map/reduce phase changes and algorithm complete checks.
var TrackPhasesInterval = 1 * time.Second

// Algorithm represents a single MapReduce algorithm.
// It tracks its map and reduce jobs and outputs.
//
// NOTE it implements the fmt.Stringer interface for logging and testing purposes.
type Algorithm struct {
	sync.RWMutex

	completed chan struct{} // Closed when algorithm is complete, in order to notify the master.

	mapCode, reduceCode                     protocol.AlgorithmCode  // Map / reduce source code, Javascript syntax.
	unassignedMapJobs, unassignedReduceJobs chan *Job               // Unassigned jobs are pushed to this channel.
	assignedMapJobs, assignedReduceJobs     map[protocol.JobID]*Job // Assigned jobs are cached in order to avoid assigning duplicate jobs to workers.

	// Reduce partition locations:
	//   1. Each element is a reduce partition input.
	//   2. Every partition input is a list with reduce inputs,
	//      sent as output from completed map jobs.
	partitions [][]protocol.Input

	// Used to count amount of unintialized reduce jobs in a thread-safe way.
	// A reduce job is uninitialized because its related partition hasn't yet been
	// filled with all inputs from related map jobs.
	//
	// Once all inputs for a specific partition (i.e. map job outputs) have been received,
	// an element is removed from this channel and an new reduce job is
	// initialized and pushed to unassignedReduceJobs.
	unsetReduceJobs chan struct{}

	results protocol.Input // Final results will be saved here.

	done    chan struct{}  // Closed when algorithm closes, and used to stop algorithm goroutines.
	closing sync.WaitGroup // Used to block Close() method until goroutines exit.
}

// NewAlgorithm initializes and returns a new MapReduce algorithm.
//
// It receives:
//  1. Map and Reduce algorithm code, in Javascript syntax.
//  2. Amount of input keys (lines) for each map job.
//     This will be used to distribute mappers' input using multiple map jobs.
//  3. Amount of reduce jobs (partitions).
//  4. Algorithm input, used with amount of map input keys to calculate amount of map jobs.
func NewAlgorithm(
	mapCode, reduceCode protocol.AlgorithmCode,
	inputLen, numPartitions uint64,
	totalInput protocol.Input) *Algorithm {

	// Sanity checks.
	switch {
	case totalInput == nil:
		panic("received nil input")
	case len(totalInput) == 0:
		panic("received empty input")
	case len(mapCode) == 0:
		panic("received empty map code")
	case len(reduceCode) == 0:
		panic("received empty reduce code")
	case inputLen == 0:
		panic("length of map job input must be at least 1")
	case uint64(len(totalInput)) < inputLen:
		panic("total input length is smaller than map job input length")
	case numPartitions == 0:
		panic("amount of reduce partitions must be at least 1")
	}

	// Initialize map jobs,
	// and calculate amount of map jobs according to input and map job size.
	//
	// NOTE we round up the map jobs to and integer.
	// This is in case the input doesn't equally divide across jobs,
	// having to create a single last map job for the remainder of the input.
	mapJobs := uint64(math.Ceil(float64(len(totalInput)) / float64(inputLen)))
	maps := make(chan *Job, mapJobs)

	// Distribute total input equally between map jobs.
	for job := uint64(0); job < mapJobs; job++ {
		id := protocol.JobID(uuid.NewV4())

		// Fetch current job's input slice from total input.
		input := totalInput[job*inputLen : int(math.Min(float64(len(totalInput)), float64((job+1)*inputLen)))]

		// Push new job to map jobs queue.
		maps <- newJob(id, input, 0, mapCode, true)
	}

	// Fill unset reduce jobs.
	unsetReduceJobs := make(chan struct{}, numPartitions)
	for i := uint64(0); i < numPartitions; i++ {
		unsetReduceJobs <- struct{}{}
	}

	// Fill partitions.
	partitions := make([][]protocol.Input, numPartitions, numPartitions)
	for i := uint64(0); i < numPartitions; i++ {
		partitions[i] = make([]protocol.Input, 0)
	}

	// Initialize a new algorithm, with above map jobs.
	a := Algorithm{
		completed: make(chan struct{}),

		mapCode:    mapCode,
		reduceCode: reduceCode,

		unassignedMapJobs: maps,
		assignedMapJobs:   make(map[protocol.JobID]*Job, cap(maps)),

		// Number of reduce jobs is equal to number of partitions.
		unassignedReduceJobs: make(chan *Job, numPartitions),
		assignedReduceJobs:   make(map[protocol.JobID]*Job, numPartitions),
		unsetReduceJobs:      unsetReduceJobs,
		partitions:           partitions,

		done: make(chan struct{}),
	}

	// Check when map phase has finished and create reduce jobs afterwards.
	a.closing.Add(1)
	go a.trackPhases()

	return &a
}

// Close closes all background goroutines.
// This function can be called if for some reason the algorithm needs to shut down,
// even if it is yet incomplete.
func (a *Algorithm) Close() {
	close(a.done)
	a.closing.Wait()
}

// trackPhases repeatedly checks for MapReduce phase changes:
//   1. It checks if all map jobs have been completed,
//      and initializes reduce jobs if so.
//   2. It then repeatedly checks if the algorithm has been completed,
//      and closes the dedicated notification channel if so.
func (a *Algorithm) trackPhases() {
	defer a.closing.Done()

MapPhase:
	for {
		select {
		case <-a.done:
			return
		case <-time.Tick(TrackPhasesInterval):
			if a.IsMapJobsComplete() {
				a.RLock()

				// Each partition has aggregated multiple map outputs by now.
				// We now shuffle these outputs by their keys. E.G.:
				// Partition #0: {"a", "1"}, {"b", "1"}, {"a", "2"}
				// will be shuffled to:  {"a", ["1","2"]}, {"b", ["1"]}
				for index, partition := range a.partitions {
					shuffled := make(map[string][]string)
					for _, outputs := range partition {
						for _, output := range outputs {
							shuffled[output.K] = append(shuffled[output.K], *output.V)
						}
					}

					input := make(protocol.Input, 0)
					for k, vs := range shuffled {
						input = append(input, protocol.NewReduceInputValue(k, vs))
					}

					// Create a new reduce job for current partition, and push to unassigned channel.
					id := protocol.JobID(uuid.NewV4())
					j := newJob(id, input, protocol.PartitionIndex(index), a.reduceCode, false)
					a.unassignedReduceJobs <- j
					<-a.unsetReduceJobs

					log.WithFields(log.Fields{"job": j, "partition index": index}).Info("new reduce job created")
				}

				a.RUnlock()
				break MapPhase
			}
		}
	}

	// Reduce phase: check if algorithm has been completed.
	for {
		select {
		case <-a.done:
			return
		case <-time.Tick(TrackPhasesInterval):
			if a.IsComplete() {
				close(a.completed)
				log.Warn("algorithm complete")
				return
			}
		}
	}
}

// IsComplete returns true if all map and reduce jobs have been completed.
func (a *Algorithm) IsComplete() bool {
	return a.IsMapJobsComplete() && a.IsReduceJobsComplete()
}

// IsMapJobsComplete returns true if all map jobs have been completed.
func (a *Algorithm) IsMapJobsComplete() bool {
	a.RLock()
	defer a.RUnlock()
	return len(a.unassignedMapJobs) == 0 && len(a.assignedMapJobs) == 0
}

// IsReduceJobsComplete returns true if all reduce jobs have been completed.
func (a *Algorithm) IsReduceJobsComplete() bool {
	a.RLock()
	defer a.RUnlock()
	return (len(a.unassignedReduceJobs) == 0 &&
		len(a.assignedReduceJobs) == 0 &&
		len(a.unsetReduceJobs) == 0)
}

// GetIncompleteJob returns:
//   1. An unassigned incomplete job.
//	    Returned *Job can either be a map or reduce job,
//      and no priority between job types is applied.
//   2. An error, which will be non-nil if no job could be returned.
//      This can happen if all jobs are already assigned
//      or if the algorithm is complete.
//
// If error is non-nil, job is nil.
func (a *Algorithm) GetIncompleteJob() (*Job, error) {
	select {
	case job := <-a.unassignedMapJobs:
		a.Lock()
		defer a.Unlock()
		a.assignedMapJobs[job.ID()] = job
		return job, nil
	case job := <-a.unassignedReduceJobs:
		a.Lock()
		defer a.Unlock()
		a.assignedReduceJobs[job.ID()] = job
		return job, nil
	default:
		return nil, errors.New("no unassigned jobs")
	}
}

// CancelJob marks an assigned job as unassigned.
func (a *Algorithm) CancelJob(j *Job) {
	a.logJobInfo(j, "cancelling job")
	defer a.logJobInfo(j, "job canceled")

	if j.IsComplete() {
		panic("trying to cancel a completed job")
	}

	// Remove job from map or reduce cache and re-push to unassigned channel.
	a.Lock()
	defer a.Unlock()

	if j.IsMapJob() {
		if _, ok := a.assignedMapJobs[j.ID()]; !ok {
			panic("trying to cancel an unassigned map job")
		}
		delete(a.assignedMapJobs, j.ID())
		a.unassignedMapJobs <- j
	} else {
		if _, ok := a.assignedReduceJobs[j.ID()]; !ok {
			panic("trying to cancel an unassigned reduce job")
		}
		delete(a.assignedReduceJobs, j.ID())
		a.unassignedReduceJobs <- j
	}
}

// CompleteMapJob adds a map job's partitions output to reduce partitions.
// A partition will be used as input for a reduce job when all map jobs are complete.
func (a *Algorithm) CompleteMapJob(id protocol.JobID, partitions map[protocol.PartitionIndex]protocol.Input) {
	a.Lock()
	defer a.Unlock()

	j := a.assignedMapJobs[id]

	a.logJobInfo(j, "starting to process completed map job")
	defer a.logJobInfo(j, "finished completing map job")

	j.Complete()

	// Add job's partitions output as input to reduce partitions.
	for index, inputs := range partitions {
		a.partitions[index] = append(a.partitions[index], inputs)
	}

	// Remove job from assigned map jobs.
	//
	// NOTE We do this on purpose after copying this job's partitions to the reduce partitions.
	// This is in order to avoid an edge case where reduce jobs can be
	// created before we finish copying this job's output to the partition.
	// If this happens, the reduce job will have missing input.
	delete(a.assignedMapJobs, id)
}

// CompleteReduceJob removes the reduce job from the un/assigned job queues,
// and saves its output to disk.
//
// TODO add tests.
func (a *Algorithm) CompleteReduceJob(id protocol.JobID, output protocol.Input) {
	if !a.IsMapJobsComplete() {
		panic("trying to complete reduce job but map jobs are incomplete")
	}

	a.Lock()
	defer a.Unlock()

	j := a.assignedReduceJobs[id]

	a.logJobInfo(j, "starting to process reduce job")
	defer a.logJobInfo(j, "finished processing complete reduce job")

	j.Complete()
	delete(a.assignedReduceJobs, id)

	// Copy output to results.
	// We must copy since each output's value is a string pointer.
	// We'll have string overrides if we just copy the addresses.
	for _, o := range output {
		a.results = append(a.results, protocol.NewMapInputValue(o.K, *o.V))
	}
}

// Results return the algorithm's results list.
//
// NOTE this might be nil if called before algorithm is complete.
//
// TODO test this.
func (a *Algorithm) Results() protocol.Input { return a.results }

func (a *Algorithm) logJobInfo(j *Job, msg string) { log.WithField("job", j).Info(msg) }

// CompletedChannel returns the algorithm's completion nofitication channel.
func (a *Algorithm) CompletedChannel() <-chan struct{} { return a.completed }

func (a *Algorithm) String() string {
	a.RLock()
	defer a.RUnlock()
	return fmt.Sprintf(
		"un/assigned map#reduce/unset jobs: %d/%d#%d/%d/%d",
		len(a.unassignedMapJobs), len(a.assignedMapJobs),
		len(a.unassignedReduceJobs), len(a.assignedReduceJobs),
		len(a.unsetReduceJobs))
}
