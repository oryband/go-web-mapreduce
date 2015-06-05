package algorithm

import (
	"strconv"
	"testing"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oryband/go-web-mapreduce/protocol"
)

func args() (protocol.Input, protocol.AlgorithmCode, protocol.JobID) {
	var input protocol.Input
	for i := 0; i < 5001; i++ {
		input = append(input, protocol.NewMapInputValue("", strconv.Itoa(i)))
	}

	code := protocol.AlgorithmCode("code")
	id := protocol.JobID(uuid.NewV4())

	return input, code, id
}

func TestNewJob(t *testing.T) {
	t.Parallel()

	input, code, id := args()

	// Panic checks.
	assert.Panics(t, func() { newJob(id, nil, 0, code, true) })
	assert.Panics(t, func() { newJob(id, protocol.Input{}, 0, code, true) })
	neg := -1
	assert.Panics(t, func() { newJob(id, input, protocol.PartitionIndex(neg), code, true) })
	assert.Panics(t, func() { newJob(id, input, 1, code, true) }) // Map jobs should have a zero value partition index.
	assert.Panics(t, func() { newJob(id, input, 0, protocol.AlgorithmCode(""), true) })

	// Proper init checks.
	assert.NotPanics(t, func() { newJob(id, input, 0, code, true) })
	assert.NotPanics(t, func() { newJob(id, input, 0, code, false) })
	assert.NotPanics(t, func() { newJob(id, input, 1, code, false) }) // Reduce jobs can have non-negative partition index.
}

func TestMapJob(t *testing.T) {
	t.Parallel()
	testJob(t, true)
}
func TestReduceJob(t *testing.T) {
	t.Parallel()
	testJob(t, false)
}

// TestJob creates a new Job and tests all its values and functions are set correctly.
func testJob(t *testing.T, isMapJob bool) {
	input, code, id := args()
	var j *Job
	meta := protocol.Meta{
		Type:  protocol.NewJob,
		JobID: id,
	}

	if isMapJob {
		require.NotPanics(t, func() { j = newJob(id, input, 0, code, isMapJob) })
		require.True(t, j.IsMapJob())
		require.False(t, j.IsReduceJob())
		meta.JobType = protocol.MapJob
	} else { // Reduce job.
		require.NotPanics(t, func() { j = newJob(id, input, 1, code, isMapJob) })
		require.False(t, j.IsMapJob())
		require.True(t, j.IsReduceJob())
		meta.JobType = protocol.ReduceJob

		meta.PartitionIndex = 1
	}

	require.EqualValues(t, &meta, j.NewJobMeta())

	require.Equal(t, id, j.ID())
	require.Equal(t, code, j.Code())

	require.Len(t, j.Input(), 5001)
	require.Equal(t, input, j.Input())

	require.True(t, assert.ObjectsAreEqual(code, j.Code()))
	require.True(t, assert.ObjectsAreEqual(input, j.Input()))

	require.False(t, j.IsComplete())
	require.NotPanics(t, func() { j.Complete() }, "first execution of Complete() shouldn't panic")
	require.True(t, j.IsComplete())
	// Completing a job twice should panic.
	require.Panics(t, func() { j.Complete() }, "first execution of Complete() shouldn't panic")
	// Job completion status should remain the same as before nonetheless.
	require.True(t, j.IsComplete())
}
