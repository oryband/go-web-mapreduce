package algorithm

import (
	"strconv"

	"github.com/oryband/go-web-mapreduce/protocol"
)

// NewMockAlgorithm is a helper function that initializes and returns a standard Algorithm.
func NewMockAlgorithm(totalSize, size, reduceJobs int) (
	alg *Algorithm,
	mapCode,
	reduceCode protocol.AlgorithmCode,
	input protocol.Input) {

	for i := 0; i < totalSize; i++ {
		input = append(input, protocol.InputValue(strconv.Itoa(i)))
	}
	mapCode = protocol.AlgorithmCode("map")
	reduceCode = protocol.AlgorithmCode("reduce")
	alg = NewAlgorithm(mapCode, reduceCode, uint64(size), uint64(reduceJobs), input)
	return
}
