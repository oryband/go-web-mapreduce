package main

import (
	"errors"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	valid "github.com/asaskevich/govalidator"
	"github.com/gin-gonic/gin"
	"gopkg.in/igm/sockjs-go.v2/sockjs"

	"github.com/oryband/go-web-mapreduce/master"
	"github.com/oryband/go-web-mapreduce/protocol"
	"github.com/oryband/go-web-mapreduce/worker"
)

// Custom websocket close status code.
// Used when server closes the client connection.
//
// See the following link for more info:
// https://tools.ietf.org/html/rfc6455#section-7.4.2
const StatusServerClose = 4000

var (
	masters    []*master.Master // Tracks active masters and their algorithms.
	maxWorkers = 100

	mu      sync.Mutex            // Used to access masters in a thread-safe way.
	done    = make(chan struct{}) // Used to notify "algorithm completed" listening goroutines to close.
	closing sync.WaitGroup        // Used to wait for "algorithm completed" listening goroutines to close.
)

func init() {
	// Seed random generator.
	// Used for assigning each new worker to a random available master.
	rand.Seed(time.Now().UnixNano())

	// Initialize an example algorithm.
	m := master.New(
		100,
		`(function(k,v){return ["1",k];})`,
		`(function(k,vs){var x=[]; $.each(vs,function(_,v){x+=JSON.parse(v);}); return x;})`,
		3, 2, protocol.Input{"1", "2", "3", "4", "5", "6", "7", "8", "9"})

	masters = append(masters, m)
	m.Start()
}

// Index returns the index page.
func Index(context *gin.Context) {
	context.HTML(http.StatusOK, "index.html", gin.H{
		"masters": len(masters),
	})
}

// NewAlgorithm initializes a new algorithm and master
// using given request parameters.
func NewAlgorithm(context *gin.Context) {
	// Unmarshal request.
	var req NewAlgorithmRequest
	if err := context.BindJSON(&req); err != nil {
		context.Error(err)
		context.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Validate request.
	if good, err := valid.ValidateStruct(&req); !good || err != nil {
		if err != nil {
			context.Error(err)
			context.JSON(http.StatusBadRequest, gin.H{"error": err})
		} else {
			errStr := "new algorithm request failed validation"
			context.Error(errors.New(errStr))
			context.JSON(http.StatusBadRequest, gin.H{"error": errStr})
		}

		return
	}

	// Initialize new master with given algorithm.
	mapJobs := int(math.Ceil(float64(len(req.Input)) / float64(req.MapInputLen)))
	reduceJobs := uint64(math.Max(1, float64(mapJobs/10)))

	defer func() {
		if rec := recover(); rec != nil {
			err := rec.(string)
			context.Error(errors.New(err))
			context.JSON(http.StatusBadRequest, gin.H{"error": err})
			return
		}
	}()

	m := master.New(
		100,
		protocol.AlgorithmCode(req.MapCode), protocol.AlgorithmCode(req.ReduceCode),
		req.MapInputLen, reduceJobs, req.Input)

	// Start master and append it to the list of tracked masters.
	m.Start()
	log.WithField("master", m).Info("master started")

	mu.Lock()
	masters = append(masters, m)
	mu.Unlock()

	// Wait for master to complete its algorithm,
	// then remove master from masters list
	closing.Add(1)
	go func(m *master.Master) {
		defer closing.Done()

		select {
		case <-done:
		case <-m.CompletedChannel():
			log.WithField("master", m).Warn("master's algorithm is complete")
		}

		var mm *master.Master
		var i int

		mu.Lock()
		for i, mm = range masters {
			if mm.ID() == m.ID() {
				masters[i], masters[len(masters)-1], masters = masters[len(masters)-1], nil, masters[:len(masters)-1]
				break
			}
		}
		mu.Unlock()

		mm.Close()
	}(m)

	context.String(http.StatusOK, "")
}

// NewWorker initializes a new SockJS worker,
// and assigns it to one of the available masters (algorithms).
func NewWorker(context *gin.Context) {
	handler := sockjs.NewHandler("/worker", sockjs.DefaultOptions, func(session sockjs.Session) {
		go func() {
			if len(masters) == 0 {
				log.Warn("no masters available for new worker")
				session.Close(StatusServerClose, "no algorithms available")
				return
			}

			m := masters[rand.Intn(len(masters))]
			log.WithField("master", m).Info("assigning worker to master")
			if err := m.AddWorker(&worker.SockJSInitializer{Session: session}); err != nil {
				log.Error(err)
			}
			log.WithField("master", m).Info("worker assigned to master")
		}()
	})

	handler.ServeHTTP(context.Writer, context.Request)
}
