// Package worker implements various MapReduce remote workers.
package worker

import (
	"fmt"
	"time"

	"github.com/oryband/go-web-mapreduce/algorithm"
	"github.com/oryband/go-web-mapreduce/protocol"
)

const (
	// SendChanSize is the sent messages queue size.
	SendChanSize = 64

	// ReceiveChanSize is the received messages queue size.
	ReceiveChanSize = SendChanSize

	// CloseMeChanSize is the size of the "close me" notification channel,
	// which the worker can use to request the master to close the worker.
	// There are multiple places in the code where the worker
	// can request the master to close it.
	// Thus, a buffered channel with at least a couple of slots available is necessary.
	CloseMeChanSize = 3
)

// JobTTL is a job's time to complete.
// The job is canceled and the worker is closed if it expires.
//
// This is a var and not const so it can be changed in unit tests.
var JobTTL = 1 * time.Minute

// Worker is an active MapReduce worker client.
// It handles its assigned job, and communication.
//
// NOTE it also implements fmt.Stringer interface for logging purposes.
type Worker interface {
	fmt.Stringer // Implements String().

	ID() protocol.WorkerID // Return the worker's identifier.

	ReceiveChannel() <-chan *protocol.Message // Returns the channel to read worker messages from.
	CloseMeChannel() <-chan struct{}          // Returned channel closes if the worker needs the master to shut it down.

	// AssignJob sends a new job's input to the worker for processing,
	// and sets a job expiration TTL.
	AssignJob(*algorithm.Job)

	GetJob() *algorithm.Job // Returns the worker's current assigned job. Returns nil if no job is assigned.
	CompleteJob()           // Stops worker's current job expiration TTL.

	// Close shutdowns the the worker by stopping message read and write pumps,
	// job expiration TTL goroutines, and closing the worker connection.
	Close()
}

// Initializer creates new workers.
//
// Each type of worker creator (basic, SockJS, etc.) should implement this interface
// in order for the master to be able to work with it.
type Initializer interface {
	NewWorker() (Worker, error)
}
