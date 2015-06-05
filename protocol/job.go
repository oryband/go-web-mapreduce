package protocol

import (
	"bytes"
	"errors"

	"github.com/satori/go.uuid"
)

// JobID is a Job's identifying ID,
// used to avoid assigning duplicate job to workers.
//
// NOTE it implementes fmt.Stringer interface for logging purposes,
// and it also has a Bytes() function which returns the []byte string representation.
type JobID uuid.UUID

var jobIDStrLen int // Used for sanity check when unmarshalling JobID string.

// init initializes jobIDLen value.
func init() { jobIDStrLen = len(uuid.NewV4().String()) }

// String returns job ID's string representation.
func (id JobID) String() string { return uuid.UUID(id).String() }

// MarshalJSON implements the json.Marshaler interface.
// A custom implementation is necessary because we want to convert the internal
// enum to string, which is more readable.
func (id *JobID) MarshalJSON() ([]byte, error) {
	buf := bytes.NewBuffer([]byte(`"`))
	buf.WriteString(uuid.UUID(*id).String()) // Output UUID in string form (38 chars).
	buf.WriteString(`"`)
	return buf.Bytes(), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
// A custom implementation is necessary because we want to validate that the
// value is a valid UUID.
func (id *JobID) UnmarshalJSON(b []byte) error {
	// Sanity check. +2 for wrapping quotes.
	if len(b) != jobIDStrLen+2 {
		return errors.New("invalid uuid string")
	}

	// Remove wrapping quotes from before converting to uuid,
	// i.e. `"de305d54-75b4-431b-adb2-eb6b9e546014"` --> `de305d54-75b4-431b-adb2-eb6b9e546014`
	u, err := uuid.FromString(string(b[1 : len(b)-1]))
	if err != nil {
		return err
	}
	*id = JobID(u)
	return nil
}

// JobType is a const type which represents map or reduce job types.
//
// NOTE it implements fmt.Stringer interface for logging purposes.
type JobType int

const (
	MapJob JobType = iota
	ReduceJob
)

// jobStrings is a string representation array of JobTypes, for logging purposes.
//
//   JobString[MapJob] = "map"
//   JobString[ReduceJob] = "reduce"
var jobStrings = []string{"map", "reduce"}

// String returns job type's string representation.
func (t JobType) String() string { return jobStrings[t] }

// MarshalJSON implements the json.Marshaler interface.
// A custom implementation is necessary because we want to convert the internal
// enum to string, which is more readable.
func (t JobType) MarshalJSON() ([]byte, error) {
	buf := bytes.NewBuffer([]byte(`"`))
	buf.WriteString(t.String())
	buf.WriteString(`"`)
	return buf.Bytes(), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
// A custom implementation is necessary because we want to convert the string
// back to the internal enum type.
func (t *JobType) UnmarshalJSON(b []byte) error {
	// Sanity check.
	if len(b) < 2 {
		return errors.New("invalid job type")
	}

	// Remove wrapping quotes from string.
	s := string(b[1 : len(b)-1])
	for i, jt := range jobStrings {
		if jt == s {
			*t = JobType(i)
			return nil
		}
	}

	return errors.New("invalid job type")
}
