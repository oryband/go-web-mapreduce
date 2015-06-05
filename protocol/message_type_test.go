package protocol

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalType(t *testing.T) {
	t.Parallel()

	for _, typ := range []MessageType{JobComplete, NewJob} {
		b, err := json.Marshal(typ)
		require.NoError(t, err)
		assert.Equal(t, `"`+typ.String()+`"`, string(b))
	}
}

func TestUnmarshalType(t *testing.T) {
	t.Parallel()

	var typ MessageType

	// Test bad inputs.
	var err error
	require.NotPanics(t, func() { err = json.Unmarshal([]byte(""), &typ) })
	require.Error(t, err)
	require.NotPanics(t, func() { err = json.Unmarshal([]byte("xxx"), &typ) })
	require.Error(t, err)

	// Test good inputs.
	for _, m := range []MessageType{JobComplete, NewJob} {
		b := []byte(`"` + m.String() + `"`)
		require.NotPanics(t, func() { err = json.Unmarshal(b, &typ) })
		require.NoError(t, err)
		assert.Equal(t, m, typ)
	}
}
