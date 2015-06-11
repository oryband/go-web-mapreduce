// Package test implements public testing utility functions, used for in other packages unit tests.
//
// File name doesn't end with *_test.go because of other packages using it.
package test

import (
	"fmt"
	"runtime"

	"github.com/oryband/go-web-mapreduce/protocol"
)

// MockMapJobOutput mocks a map job's output.
// Mocked value is in the following form:
//
// Each value for each partition is a 5-digit number:
//   1. There are 1001 output lines for every partition.
//   2. Every output line's value is in the following form:
//      1. The fifth digit is the partition number.
//		2. The rest 4 digits are the lines number.
//		3. For example, input for partition #3: 30000, 30001, .., 31001.
//
// This is in order to make the map job's output different
// from its input created in newAlgorithm(totalInputSize, inputSize).
// Thus, we can be sure the input doesn't get mixed up with output.
func MockMapJobOutput(totalSize, size int, partitions []int) map[protocol.PartitionIndex]protocol.Input {
	ps := make(map[protocol.PartitionIndex]protocol.Input, totalSize)
	for _, i := range partitions {
		pi := protocol.PartitionIndex(i)
		ps[pi] = make(protocol.Input, 0) // pi means Partition Index
		for j := 0; j < size; j++ {
			ps[pi] = append(ps[pi], protocol.NewMapInputValue("", fmt.Sprintf("%.5f", 10000*i+j)))
		}
	}
	return ps
}

// MockReduceJobOutput mocks a reduce job's output, similar to mockMapJobOutput.
func MockReduceJobOutput(size int) protocol.Input {
	var output protocol.Input
	for i := 0; i < size; i++ {
		output = append(output, protocol.NewMapInputValue("", fmt.Sprintf("%.3f", i)))
	}
	return output
}

// CallingFunctionInfo is a helper function which returns the calling
// function's name and line, according to skip argument.
func CallingFunctionInfo(skip int) string {
	if pc, _, line, ok := runtime.Caller(skip); ok {
		// Add function name if possible, using program counter.
		if fun := runtime.FuncForPC(pc); fun != nil {
			return fmt.Sprintf("%s line %d", fun.Name(), line)
		}
		return fmt.Sprintf("line %d", line)
	}
	return ""
}
