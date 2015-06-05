package protocol

import (
	"encoding/json"
	"testing"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalJobID(t *testing.T) {
	t.Parallel()

	id := uuid.NewV4()
	b, err := json.Marshal(&id)
	require.NoError(t, err)
	assert.Equal(t, `"`+id.String()+`"`, string(b))
}

func TestUnmarshalJobID(t *testing.T) {
	t.Parallel()

	var id JobID

	// Test bad string i.e. not a uuid.
	var err error
	assert.NotPanics(t, func() { err = json.Unmarshal([]byte(""), &id) })
	assert.Error(t, err)
	assert.NotPanics(t, func() { err = json.Unmarshal([]byte(`"`+"123"+`"`), &id) })
	assert.Error(t, err)

	// Test good uuid.
	uid := uuid.NewV4()
	require.NoError(t, json.Unmarshal([]byte(`"`+uid.String()+`"`), &id))
	assert.Equal(t, JobID(uid), id)
}

func TestMarshalJobType(t *testing.T) {
	t.Parallel()

	for _, jt := range []JobType{MapJob, ReduceJob} {
		b, err := json.Marshal(jt)
		require.NoError(t, err)
		assert.Equal(t, `"`+jt.String()+`"`, string(b))
	}
}

func TestUnmarshalJobType(t *testing.T) {
	t.Parallel()

	var jt JobType

	// Test bad inputs.
	var err error
	require.NotPanics(t, func() { err = json.Unmarshal([]byte(""), &jt) })
	require.Error(t, err)
	require.NotPanics(t, func() { err = json.Unmarshal([]byte(`"x"`), &jt) })
	require.Error(t, err)

	// Test good inputs.
	for i, b := range jobStrings {
		require.NoError(t, json.Unmarshal([]byte(`"`+b+`"`), &jt))
		assert.Equal(t, JobType(i), jt)
	}
}
