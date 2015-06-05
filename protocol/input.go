// Here be input related data types.

package protocol

// Input is a single algorithm.Job's input, be it a map or reduce job.
type Input []InputValue

// InputValue is the underlying data used to process the input.
type InputValue string

// PartitionIndex is a reduce job's partition index.
type PartitionIndex uint64
