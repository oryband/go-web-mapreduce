// Here be the BaseWorker test suite executing the Worker interface unit tests.

package worker

import (
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

const logLevel = log.ErrorLevel

type BaseWorkerSuite struct{ *WorkerSuite }

// TestBaseWorkerSuite executes the Worker test suite for the BaseWorker.
func TestBaseWorkerSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, &BaseWorkerSuite{WorkerSuite: new(WorkerSuite)})
}

func (s *BaseWorkerSuite) SetupSuite() {
	log.SetLevel(logLevel)

	s.WorkerSuite.SetupSuite()
}

// SetupTest initializes a BaseWorker, and calls WorkerSuite's SetupTest().
func (s *BaseWorkerSuite) SetupTest() {
	w, err := new(BaseInitializer).NewWorker()
	s.Require().NoError(err)
	s.WorkerSuite.worker = w

	s.WorkerSuite.SetupTest()
}

func (s *BaseWorkerSuite) TeardownTest() { s.WorkerSuite.TearDownTest() }
