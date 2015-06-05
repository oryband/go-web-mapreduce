package protocol

import (
	"bytes"
	"errors"
)

// MessageType is a const type which represents various message types
// used in master-worker communication.
type MessageType int

const (
	JobComplete MessageType = iota // Sent from worker to master, notifying worker completed its job.
	NewJob                         // Sent from master to worker, assigning a new job.
)

// messageTypeStrings is a string representation of message types,
// for logging purposes.
var messageTypeStrings = []string{"job complete", "new job"}

// String returns message type's string representation.
func (t MessageType) String() string { return messageTypeStrings[t] }

// MarshalJSON implements the json.Marshaler interface.
// A custom implementation is necessary because we want to convert the internal
// enum to string, which is more readable.
func (t MessageType) MarshalJSON() ([]byte, error) {
	buf := bytes.NewBuffer([]byte(`"`))
	buf.WriteString(t.String())
	buf.WriteString(`"`)
	return buf.Bytes(), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
// A custom implementation is necessary because we want to convert the string
// back to the internal enum type.
func (t *MessageType) UnmarshalJSON(b []byte) error {
	if t == nil {
		t = new(MessageType)
	}

	// Sanity check.
	if len(b) < 2 {
		return errors.New("invalid message type")
	}

	// Remove wrapping quotes from string.
	s := string(b[1 : len(b)-1])
	for i, m := range messageTypeStrings {
		if m == s {
			*t = MessageType(i)
			return nil
		}
	}

	return errors.New("invalid message type")
}
