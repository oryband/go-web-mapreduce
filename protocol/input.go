// Here be input related data types.

package protocol

// Input is a single job's input, be it a map or reduce job.
type Input []*InputValue

// InputValue is either a map or reduce jobs single input element,
// i.e.  a key-value pair.
type InputValue struct {
	K  string    `json:"key"`
	V  *string   `json:"value,omitempty"`  // Non-nil for map inputs, reduce outputs.
	VS *[]string `json:"values,omitempty"` // Non-nil for map outputs, reduce inputs.
}

// NewMapInputValue returns a new InputValue wit it's key and value fields set,
// and the values field unset.
func NewMapInputValue(k, v string) *InputValue {
	if k == "" && v == "" {
		panic("both key and value are empty")
	}
	return &InputValue{K: k, V: &v, VS: nil}
}

// NewReduceInputValue returns a new InputValue wit it's key and values fields set,
// and the value field unset.
func NewReduceInputValue(k string, vs []string) *InputValue {
	if vs == nil {
		panic("given values []string is nil")
	}
	return &InputValue{K: k, V: nil, VS: &vs}
}
