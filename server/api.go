// Here be API requests and response objects.

package main

import "github.com/oryband/go-web-mapreduce/protocol"

// NewAlgorithmRequest is sent from the client when he wishes to add a new algorithm.
type NewAlgorithmRequest struct {
	MapInputLen uint64                 `json:"map_input_length"`
	MapCode     protocol.AlgorithmCode `json:"map_code" valid:"required"`
	ReduceCode  protocol.AlgorithmCode `json:"reduce_code" valid:"required"`
	Input       protocol.Input         `json:"input" valid:"required"`
}
