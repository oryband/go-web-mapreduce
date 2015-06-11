package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/suite"

	"github.com/oryband/go-web-mapreduce/protocol"
)

const (
	ginTestMode = "test"
	logLevel    = log.ErrorLevel
)

type ServerSuite struct {
	suite.Suite
	engine *gin.Engine
}

func TestServerSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(ServerSuite))
}

func (s *ServerSuite) SetupSuite() {
	log.SetLevel(logLevel)
	gin.SetMode(ginTestMode)
}

func (s *ServerSuite) SetupTest() {
	done = make(chan struct{})
	closing = sync.WaitGroup{}

	// Init gin server.
	s.engine = gin.New()
	s.engine.POST("/algorithm", NewAlgorithm)
}

func (s *ServerSuite) TearDownTest() {
	close(done)

	d := make(chan struct{})
	go func() {
		closing.Wait()
		close(d)
	}()

	select {
	case <-d:
	case <-time.After(100 * time.Millisecond):
		s.Fail("all algorithm complete goroutines didn't close fast enough")
	}
}

func (s *ServerSuite) TestAddAlgorithm() {
	// Generate request.
	req := NewAlgorithmRequest{
		MapInputLen: 1,
		MapCode:     `"hello world map"`,
		ReduceCode:  `"hello world reduce"`,
		Input:       protocol.Input{protocol.NewMapInputValue("", "1"), protocol.NewMapInputValue("", "2")},
	}

	b, err := json.Marshal(&req)
	s.Require().NoError(err)
	r, err := http.NewRequest("POST", "/algorithm", bytes.NewBuffer(b))
	s.Require().NoError(err)

	// Send request.
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.engine.ServeHTTP(w, r)

	s.Require().Equal(http.StatusOK, w.Code)
	s.Require().Equal(w.Body.String(), "")
}
