// Here be an implementation of Worker using a SockJS connection.

package worker

import (
	"encoding/json"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
	"gopkg.in/igm/sockjs-go.v2/sockjs"

	"github.com/oryband/go-web-mapreduce/protocol"
)

// Custom websocket close status code:
// Used when server closes the client connection.
//
// See the following link for more info:
// https://tools.ietf.org/html/rfc6455#section-7.4.2
const StatusWorkerClose = 4000

// SockJSInitializer implements the Initializer interface,
// and can initialize SockJS workers.
type SockJSInitializer struct{ Session sockjs.Session }

// NewWorker creates a new SockJS Worker, using an active client HTTP connection.
// It also receive a send-only channel, which will be closed (thus become readable)
// if the worker needs close due to some error.
func (d *SockJSInitializer) NewWorker() (Worker, error) {
	base, _ := new(BaseInitializer).NewWorker()
	worker := sockjsWorker{
		BaseWorker: base.(*BaseWorker),
		session:    d.Session,
		done:       make(chan struct{}),
	}

	// Start worker read/write message pumps,
	// and close when finished or on any error.
	go worker.sendPump()
	go worker.receivePump()

	return &worker, nil
}

// sockjsWorker implements the Worker interface,
// communicating with the client via a SockJS connection.
type sockjsWorker struct {
	*BaseWorker

	session sockjs.Session // SockJS connection.

	done    chan struct{}  // Closed when worker closes. Used to signal receive pump to shut down.
	closing sync.WaitGroup // Used to block Close() untill all goroutines exit.
}

// Close stops message read/write pumps and closes the SockJS connection.
func (w *sockjsWorker) Close() {
	log.WithField("worker", w).Info("closing worker")
	defer log.WithField("worker", w).Info("worker successfully closed")

	w.BaseWorker.Close()

	// Gracefully close SockJS connection.
	err := w.session.Close(StatusWorkerClose, "master is closing the worker")
	if err != nil {
		log.WithField("worker", w).Error(err)
	}

	// Stop send and receive pumps.
	close(w.done)
	w.closing.Wait()
}

// sendPump repeatedly fetches messages piped from the master,
// and sends it to the worker via its SockJS connection.
//
// It also periodcally pings a heartbeat message.
func (w *sockjsWorker) sendPump() {
	w.closing.Add(1)
	defer w.closing.Done()

	defer close(w.Send)
	for {
		select {
		case <-w.done:
			return
		case message := <-w.Send:
			b, err := json.Marshal(message)
			if err != nil {
				log.WithFields(log.Fields{"worker": w, "message": message}).Error(err)
			}
			if err := w.session.Send(string(b)); err != nil {
				log.WithField("worker", w).Error(err)
				w.CloseMe <- struct{}{}
				return
			}
		}
	}
}

// receivePump repeatedly reads messages from the SockJS connection,
// and pipes them to the master.
func (w *sockjsWorker) receivePump() {
	w.closing.Add(1)
	defer w.closing.Done()

	defer close(w.Receive)

	// Keep reading until notified to close or on any error.
	recv := make(chan struct{})
	var message *protocol.Message
	var str string
	var err error
	for {
		// Notify when a message has received.
		// This is necessary selecting from the network and the "close" channel together.
		go func() {
			str, err = w.session.Recv()
			recv <- struct{}{}
		}()

		select {
		case <-w.done:
			return
		case <-recv:
			if err != nil {
				log.WithField("worker", w).Error(err)
				w.CloseMe <- struct{}{}
				return
			}

			if err := json.Unmarshal([]byte(str), &message); err != nil {
				log.WithField("worker", w).Error(err)
				w.CloseMe <- struct{}{}
				return
			}

			w.Receive <- message
		}
	}
}

func (w *sockjsWorker) String() string {
	return strings.Join([]string{"sockjs/", w.BaseWorker.String()}, "")
}
