package algorithm

import (
	"math"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/oryband/go-web-mapreduce/test"
)

// benchmarkSerialAlgorithm creates and completes an algorithm with serial job fetching and completion,
// using given input, map, and reduce jobs sizes.
func benchmarkSerialAlgorithm(b *testing.B, totalInputSize, inputSize, reduceJobs, mockMapOutputSize, mockReduceOutputSize int) {
	for i := 0; i < b.N; i++ {
		serialAlgorithmCycle(totalInputSize, inputSize, reduceJobs, mockMapOutputSize, mockReduceOutputSize)
	}
}

// benchmarkConcurrentAlgorithm creates and completes an algorithm with concurrent job fetching completion,
// using given input, map, and reduce jobs sizes.
func benchmarkConcurrentAlgorithm(b *testing.B, totalInputSize, inputSize, reduceJobs, mockMapOutputSize, mockReduceOutputSize int) {
	for i := 0; i < b.N; i++ {
		concurrentAlgorithmCycle(totalInputSize, inputSize, reduceJobs, mockMapOutputSize, mockReduceOutputSize)
	}
}

// Get and complete jobs serially, one-by-one.
func serialAlgorithmCycle(totalInputSize, inputSize, reduceJobs, mockMapOutputSize, mockReduceOutputSize int) {
	a, _, _, _ := NewMockAlgorithm(totalInputSize, inputSize, reduceJobs)
	mapJobs := int(math.Ceil(float64(totalInputSize) / float64(inputSize)))

	// Get and complete all map jobs.
	for i := 0; i < mapJobs; i++ {
		j, _ := a.GetIncompleteJob()
		a.CompleteMapJob(j.ID(), test.MockMapJobOutput(totalInputSize, mockMapOutputSize, []int{0, 1, 3, 4}))
	}

	// Wait until reduce phase has started i.e. all reduce jobs have been initialized.
	for range time.Tick(1 * time.Nanosecond) {
		if len(a.unsetReduceJobs) == 0 {
			break
		}
	}

	// Get and complete all reduce jobs.
	for i := 0; i < reduceJobs; i++ {
		j, _ := a.GetIncompleteJob()
		a.CompleteReduceJob(j.ID(), test.MockReduceJobOutput(mockReduceOutputSize))
	}
}

// Get and complete jobs concurrently.
func concurrentAlgorithmCycle(totalInputSize, inputSize, reduceJobs, mockMapOutputSize, mockReduceOutputSize int) {
	rand.Seed(time.Now().UnixNano())

	a, _, _, _ := NewMockAlgorithm(totalInputSize, inputSize, reduceJobs)

	// Get and complete jobs concurrently.
	jobs := make(chan *Job, mapJobs+reduceJobs)
	wg := sync.WaitGroup{}
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer wg.Done()

			for {
				runtime.Gosched()

				select {
				case <-a.CompletedChannel():
					return
				case j := <-jobs:
					// Have a 30% probabilty to cancel the job, else complete it.
					if rand.Intn(100) < 30 {
						a.CancelJob(j)
						continue
					}

					if j.IsMapJob() {
						a.CompleteMapJob(j.ID(), test.MockMapJobOutput(totalInputSize, mockMapOutputSize, []int{0, 1, 3, 4}))
					} else { // Reduce job.
						a.CompleteReduceJob(j.ID(), test.MockReduceJobOutput(mockReduceOutputSize))
					}
				}
			}
		}()
	}

	// Get incomplete jobs concurrently.
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer wg.Done()

			for {
				runtime.Gosched()

				select {
				case <-a.CompletedChannel():
					return
				default:
					j, err := a.GetIncompleteJob()
					switch {
					case err != nil:
					case j != nil:
						jobs <- j
					}
				}
			}
		}()
	}

	wg.Wait()
}

func BenchmarkSerialAlgorithm1(b *testing.B) { benchmarkSerialAlgorithm(b, 1000, 100, 20, 1000, 1000) }
func BenchmarkSerialAlgorithm2(b *testing.B) { benchmarkSerialAlgorithm(b, 10000, 100, 20, 1000, 1000) }
func BenchmarkSerialAlgorithm3(b *testing.B) { benchmarkSerialAlgorithm(b, 20000, 100, 20, 1000, 1000) }
func BenchmarkSerialAlgorithm4(b *testing.B) { benchmarkSerialAlgorithm(b, 20000, 1000, 20, 1000, 1000) }
func BenchmarkSerialAlgorithm5(b *testing.B) {
	benchmarkSerialAlgorithm(b, 20000, 10000, 20, 1000, 1000)
}
func BenchmarkSerialAlgorithm6(b *testing.B) {
	benchmarkSerialAlgorithm(b, 20000, 20000, 20, 1000, 1000)
}
func BenchmarkSerialAlgorithm7(b *testing.B) {
	benchmarkSerialAlgorithm(b, 20000, 20000, 200, 1000, 1000)
}
func BenchmarkSerialAlgorithm8(b *testing.B) {
	benchmarkSerialAlgorithm(b, 20000, 20000, 2000, 1000, 1000)
}

func BenchmarkConcurrentAlgorithm1(b *testing.B) {
	benchmarkConcurrentAlgorithm(b, 1000, 100, 20, 1000, 1000)
}
func BenchmarkConcurrentAlgorithm2(b *testing.B) {
	benchmarkConcurrentAlgorithm(b, 10000, 100, 20, 1000, 1000)
}
func BenchmarkConcurrentAlgorithm3(b *testing.B) {
	benchmarkConcurrentAlgorithm(b, 20000, 100, 20, 1000, 1000)
}
func BenchmarkConcurrentAlgorithm4(b *testing.B) {
	benchmarkConcurrentAlgorithm(b, 20000, 1000, 20, 1000, 1000)
}
func BenchmarkConcurrentAlgorithm5(b *testing.B) {
	benchmarkConcurrentAlgorithm(b, 20000, 10000, 20, 1000, 1000)
}
func BenchmarkConcurrentAlgorithm6(b *testing.B) {
	benchmarkConcurrentAlgorithm(b, 20000, 20000, 20, 1000, 1000)
}
func BenchmarkConcurrentAlgorithm7(b *testing.B) {
	benchmarkConcurrentAlgorithm(b, 20000, 20000, 200, 1000, 1000)
}
func BenchmarkConcurrentAlgorithm8(b *testing.B) {
	benchmarkConcurrentAlgorithm(b, 20000, 20000, 2000, 1000, 1000)
}
func BenchmarkConcurrentAlgorithm9(b *testing.B) {
	benchmarkConcurrentAlgorithm(b, 20000, 20000, 20000, 10000, 1000)
}
func BenchmarkConcurrentAlgorithm10(b *testing.B) {
	benchmarkConcurrentAlgorithm(b, 20000, 20000, 20000, 20000, 1000)
}
