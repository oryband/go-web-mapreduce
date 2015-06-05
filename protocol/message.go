package protocol

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// MapOutput is the map job output sent from the worker to the master.
// It is a map and not a slice in order to save space,
// by skipping allocating space for partitions with no output.
type MapOutput map[PartitionIndex]Input

// MarshalJSON implements json.Marshaler interface.
// A custom implementation is necessary in order to convert
// PartitionIndex keys from uint64 to string.
//
// This is because JSON keys must be strings, and we want the indexes to be
// uint64 internally.
func (ps *MapOutput) MarshalJSON() ([]byte, error) {
	if *ps == nil {
		panic("given MapOutput is nil")
	}
	out := make(map[string]Input)
	for index, input := range *ps {
		out[strconv.FormatUint(uint64(index), 10)] = input
	}
	return json.Marshal(out)
}

// UnmarshalJSON implements json.Unmarshaler interface.
// A custom implementation is necessary in order to convert PartitionIndex keys
// from string back to uint64.
//
// This is because JSON keys must be strings, and we want the indexes to be
// uint64 internally.
func (ps *MapOutput) UnmarshalJSON(b []byte) error {
	if *ps == nil {
		*ps = make(MapOutput)
	}

	var m map[string]Input
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}

	var i uint64
	var err error
	for index, input := range m {
		if i, err = strconv.ParseUint(index, 10, 64); err != nil {
			return err
		}
		(*ps)[PartitionIndex(i)] = input
	}

	return nil
}

// Message is the data object sent and received between master and worker.
//
// NOTE it implements fmt.Stringer interface for logging purposes.
type Message struct {
	Meta         *Meta         `json:"meta"`
	Input        Input         `json:"input,omitempty"`         // Input originates from server. Missing in messages from worker.
	MapOutput    MapOutput     `json:"map_output,omitempty"`    // Output originates from worker. Missing in messages from master.
	ReduceOutput Input         `json:"reduce_output,omitempty"` // Like MapOutput, but for reduce workers.
	Code         AlgorithmCode `json:"code"`                    // Code for the worker to execute on input.
}

// Meta is a message associated metadata.
// E.g. intermediary key location upon completing a map job.
type Meta struct {
	Type    MessageType `json:"type"`    // Message type.
	JobType JobType     `json:"jobType"` // Map or Reduce job.
	JobID   JobID       `json:"jobID"`   // Job ID whose message relates too.

	// Reduce job's partition input index. Missing in messages from worker.
	// The reason this is a string type is so we could use "omitempty" struct tag and
	// check for invalid values.
	// If we would've made this an int, partition #0 would be omitted
	// and we couldn't be sure that it was set properly when calling NewMessage().
	PartitionIndex PartitionIndex `json:"partition,omitempty"`
}

// NewMessage returns a new message using given metadata type and data.
func NewMessage(input Input, code AlgorithmCode, meta *Meta) *Message {
	// Sanity checks.
	switch {
	case meta == nil:
		panic("meta is nil")
	case meta.JobType == MapJob && meta.PartitionIndex != 0:
		panic("received non-zero Meta.Partition with map job message")
	case input == nil:
		panic("received nil job input")
	case len(code) == 0:
		panic("received empty algorithm code")
	}

	return &Message{Meta: meta, Input: input, Code: code}
}

func (m *Message) String() string {
	return fmt.Sprintf("%s/%s/%s", m.Meta.Type, m.Meta.JobType, m.Meta.JobID)
}
