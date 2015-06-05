package protocol

import "github.com/satori/go.uuid"

// WorkerID is a worker's identifying ID.
//
// NOTE it implementes fmt.Stringer interface for logging purposes.
type WorkerID uuid.UUID

func (id WorkerID) String() string { return uuid.UUID(id).String() }
