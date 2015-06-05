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
		input = append(input, NewMapInputValue("", strconv.Itoa(i)))
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

	output := mapOutput()
	b, err := json.Marshal(&output)
	require.NoError(t, err)
	assert.Equal(t, mapOutputString(), string(b))
}

func TestUnmarshalMapOutput(t *testing.T) {
	t.Parallel()

	out := make(MapOutput)

	// Test bad input.
	var err error
	require.NotPanics(t, func() {
		err = json.Unmarshal([]byte(`{"0":[{"key":"","value":"1"},{"key":"","value":"2"}],"xxx":[{"key":"","value":"3"},{"key":"","value":"4"}]}`), &out)
	})
	require.Error(t, err)

	// Test goot input.
	b := []byte(mapOutputString())
	require.NoError(t, json.Unmarshal(b, &out))
	assert.Equal(t, mapOutput(), out)
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
		Input: reduceOutput(),
		Code:  AlgorithmCode("code"),
	}

	// Test marshalling a map job message.
	b, err := json.Marshal(&m)
	require.NoError(t, err)
	assert.Equal(t, `{"meta":{"type":"job complete","jobType":"map","jobID":"`+id.String()+`"},"input":[{"key":"","value":"1"},{"key":"","value":"2"},{"key":"","value":"3"}],"code":"code"}`, string(b))

	// Test marshalling a reduce job message.
	m.Meta.PartitionIndex = 1
	m.Meta.JobType = ReduceJob
	b, err = json.Marshal(&m)
	require.NoError(t, err)
	assert.Equal(t, `{"meta":{"type":"job complete","jobType":"reduce","jobID":"`+id.String()+`","partition":1},"input":[{"key":"","value":"1"},{"key":"","value":"2"},{"key":"","value":"3"}],"code":"code"}`, string(b))
}

func TestUnmarshalMessage(t *testing.T) {
	t.Parallel()

	id := uuid.NewV4()

	s := `{"meta":{"type":"job complete","jobType":"map","jobID":"` + id.String() + `"},"input":[{"key":"","value":"1"},{"key":"","value":"2"},{"key":"","value":"3"}],"code":"code"}`

	mm := Message{
		Meta: &Meta{
			Type:    JobComplete,
			JobType: MapJob,
			JobID:   JobID(id),
		},
		Input: reduceOutput(),
		Code:  AlgorithmCode("code"),
	}

	var m Message
	var err error
	require.NotPanics(t, func() { err = json.Unmarshal([]byte(s), &m) })
	require.NoError(t, err)
	require.Equal(t, &mm, &m)

	s = `{"meta":{"type":"job complete","jobType":"map","jobID":"` + id.String() + `"},"map_output":` + mapOutputString() + `,"code":"code"}`
	mm.Input = nil
	mm.MapOutput = mapOutput()
	m = Message{}
	require.NotPanics(t, func() { err = json.Unmarshal([]byte(s), &m) })
	require.NoError(t, err)
	require.Equal(t, &mm, &m)

	s = `{"meta":{"type":"job complete","jobType":"reduce","jobID":"` + id.String() + `","partition":1},"reduce_output":[{"key":"","value":"1"},{"key":"","value":"2"},{"key":"","value":"3"}],"code":"code"}`
	mm.Meta.PartitionIndex = 1
	mm.Meta.JobType = ReduceJob
	mm.MapOutput = nil
	mm.ReduceOutput = reduceOutput()
	m = Message{}
	require.NotPanics(t, func() { err = json.Unmarshal([]byte(s), &m) })
	require.NoError(t, err)
	require.Equal(t, &mm, &m)
}

// mapOutput is a helper function that returns a stub MapOutput for testing.
func mapOutput() MapOutput {
	return MapOutput{
		PartitionIndex(0): Input{NewReduceInputValue("", []string{"1", "2"}), NewReduceInputValue("", []string{"3", "4"})},
		PartitionIndex(3): Input{NewReduceInputValue("", []string{"5", "6"}), NewReduceInputValue("", []string{"7", "8"})}}
}

// reduceOutput is a helper function that returns a stub reduce output for testing.
func reduceOutput() Input {
	return Input{NewMapInputValue("", "1"), NewMapInputValue("", "2"), NewMapInputValue("", "3")}
}

// mapOutputString is a helper function that returns a stub MapOutput string for testing.
func mapOutputString() string {
	return `{"0":[{"key":"","values":["1","2"]},{"key":"","values":["3","4"]}],"3":[{"key":"","values":["5","6"]},{"key":"","values":["7","8"]}]}`
}
