package protocol

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockNewMessageArguments mocks NewMessage() function arguments.
// This is needed in order to tests various calls to this function.
func mockNewMessageArguments() (mc, rc AlgorithmCode, input Input, mapMeta, reduceMeta *Meta) {
	mc = AlgorithmCode("map")
	rc = AlgorithmCode("reduce")
	for i := 0; i < 5001; i++ {
		input = append(input, InputValue(strconv.Itoa(i)))
	}
	mapMeta = &Meta{
		Type:    JobComplete,
		JobType: MapJob,
		JobID:   JobID(uuid.NewV4()),
	}
	reduceMeta = &Meta{
		Type:           JobComplete,
		JobType:        ReduceJob,
		JobID:          JobID(uuid.NewV4()),
		PartitionIndex: 1,
	}
	return
}

// TestNewMessage tests various calls to NewMessage().
func TestNewMessage(t *testing.T) {
	t.Parallel()

	mc, rc, input, mapMeta, reduceMeta := mockNewMessageArguments()

	// Panic checks:

	// Map message.
	mc, rc, input, mapMeta, reduceMeta = mockNewMessageArguments()
	assert.Panics(t, func() { NewMessage(nil, mc, mapMeta) })

	mc, rc, input, mapMeta, reduceMeta = mockNewMessageArguments()
	assert.Panics(t, func() { NewMessage(input, "", mapMeta) })

	mc, rc, input, mapMeta, reduceMeta = mockNewMessageArguments()
	mapMeta.PartitionIndex = 1
	assert.Panics(t, func() { NewMessage(input, mc, mapMeta) })

	// Reduce message.
	mc, rc, input, mapMeta, reduceMeta = mockNewMessageArguments()
	assert.Panics(t, func() { NewMessage(nil, rc, reduceMeta) })

	mc, rc, input, mapMeta, reduceMeta = mockNewMessageArguments()
	assert.Panics(t, func() { NewMessage(input, "", reduceMeta) })

	// Non-panicking (i.e. good arguments) checks:

	var m *Message
	mc, rc, input, mapMeta, reduceMeta = mockNewMessageArguments()
	assert.NotPanics(t, func() { m = NewMessage(input, mc, mapMeta) })
	assert.Equal(t, input, m.Input)
	assert.Equal(t, mc, m.Code)
	assert.Equal(t, mapMeta, m.Meta)

	mc, rc, input, mapMeta, reduceMeta = mockNewMessageArguments()
	assert.NotPanics(t, func() { m = NewMessage(input, rc, reduceMeta) })
	assert.Equal(t, input, m.Input)
	assert.Equal(t, rc, m.Code)
	assert.Equal(t, reduceMeta, m.Meta)
}

func TestMarshalMapOutput(t *testing.T) {
	t.Parallel()

	output := MapOutput{PartitionIndex(0): Input{"1", "2"}, PartitionIndex(3): Input{"3", "4"}}
	b, err := json.Marshal(&output)
	require.NoError(t, err)
	assert.Equal(t, `{"0":["1","2"],"3":["3","4"]}`, string(b))
}

func TestUnmarshalMapOutput(t *testing.T) {
	t.Parallel()

	out := make(MapOutput)

	// Test bad input.
	var err error
	require.NotPanics(t, func() { err = json.Unmarshal([]byte(`{"0":["1","2"],"xxx":["3","4"]}`), &out) })
	require.Error(t, err)

	// Test goot input.
	b := []byte(`{"0":["1","2"],"3":["3","4"]}`)
	require.NoError(t, json.Unmarshal(b, &out))
	assert.Equal(t, MapOutput{PartitionIndex(0): Input{"1", "2"}, PartitionIndex(3): Input{"3", "4"}}, out)
}

func TestMarshalMessage(t *testing.T) {
	t.Parallel()

	id := uuid.NewV4()
	m := Message{
		Meta: &Meta{
			Type:    JobComplete,
			JobType: MapJob,
			JobID:   JobID(id),
		},
		Input: Input{"1", "2", "3"},
		Code:  AlgorithmCode("code"),
	}

	// Test marshalling a map job message.
	b, err := json.Marshal(&m)
	require.NoError(t, err)
	assert.Equal(t, `{"meta":{"type":"job complete","jobType":"map","jobID":"`+id.String()+`"},"input":["1","2","3"],"code":"code"}`, string(b))

	// Test marshalling a reduce job message.
	m.Meta.PartitionIndex = 1
	m.Meta.JobType = ReduceJob
	b, err = json.Marshal(&m)
	require.NoError(t, err)
	assert.Equal(t, `{"meta":{"type":"job complete","jobType":"reduce","jobID":"`+id.String()+`","partition":1},"input":["1","2","3"],"code":"code"}`, string(b))
}

func TestUnmarshalMessage(t *testing.T) {
	t.Parallel()

	id := uuid.NewV4()

	s := `{"meta":{"type":"job complete","jobType":"map","jobID":"` + id.String() + `"},"input":["1","2","3"],"code":"code"}`

	mm := Message{
		Meta: &Meta{
			Type:    JobComplete,
			JobType: MapJob,
			JobID:   JobID(id),
		},
		Input: Input{"1", "2", "3"},
		Code:  AlgorithmCode("code"),
	}

	var m Message
	var err error
	require.NotPanics(t, func() { err = json.Unmarshal([]byte(s), &m) })
	require.NoError(t, err)
	require.Equal(t, &mm, &m)

	s = `{"meta":{"type":"job complete","jobType":"map","jobID":"` + id.String() + `"},"map_output":{"0":["1","2"],"3":["3","4"]},"code":"code"}`
	mm.Input = nil
	mm.MapOutput = MapOutput{PartitionIndex(0): Input{"1", "2"}, PartitionIndex(3): Input{"3", "4"}}
	m = Message{}
	require.NotPanics(t, func() { err = json.Unmarshal([]byte(s), &m) })
	require.NoError(t, err)
	require.Equal(t, &mm, &m)

	s = `{"meta":{"type":"job complete","jobType":"reduce","jobID":"` + id.String() + `","partition":1},"reduce_output":["1","2","3"],"code":"code"}`
	mm.Meta.PartitionIndex = 1
	mm.Meta.JobType = ReduceJob
	mm.MapOutput = nil
	mm.ReduceOutput = Input{"1", "2", "3"}
	m = Message{}
	require.NotPanics(t, func() { err = json.Unmarshal([]byte(s), &m) })
	require.NoError(t, err)
	require.Equal(t, &mm, &m)
}
