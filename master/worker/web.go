// Here be an implementation of Worker using a WebSocket connection.

package worker

import (
	"net/http"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/websocket"

	"github.com/oryband/go-web-mapreduce/master/protocol"
)

// Websocket related configuration.
const (
	// TODO the following should be optimized
	writeTimeout    = 10 * time.Second       // Amount of time allowed to wait for a write to a worker to complete.
	pongTimeout     = 60 * time.Second       // Client "pong" response timeout.
	pingPeriod      = (pongTimeout * 9) / 10 // Ping message interval, must be less than pongTimeout.
	maxMessageSize  = 512                    // Maximum message size allowed to be received from a worker.
	readBufferSize  = 1024
	writeBufferSize = readBufferSize
)

// WebSocket configuration.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  readBufferSize,
	WriteBufferSize: writeBufferSize,
}

// webWorker implements the Worker interface,
// communicating with the client via a WebSocket connection.
type webWorker struct {
	*BaseWorker

	conn *websocket.Conn // WebSocket connection.

	done    chan struct{}  // Closed when worker closes. Used to signal receive pump to shut down.
	closing sync.WaitGroup // Used to block Close() untill all goroutines exit.
}

// NewWorker creates a new WebSocker Worker, using an active client HTTP connection.
// It also receive a send-only channel, which will be closed (thus become readable)
// if the worker needs close due to some error.
func NewWorker(w http.ResponseWriter, r *http.Request) (Worker, error) {
	// Upgrade HTTP connection to WebSocket.
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}

	base, _ := NewBaseWorker(w, r)
	worker := webWorker{
		BaseWorker: base.(*BaseWorker),

		conn: conn,
		done: make(chan struct{}),
	}

	// Start worker read/write message pumps,
	// and close when finished or on any error.
	go worker.sendPump()
	go worker.receivePump()

	return &worker, nil
}

// Close stops message read/write pumps and closes the WebSocket connection.
// This abandons the current executing job.
func (w *webWorker) Close() {
	log.WithField("worker", w).Info("closing web worker")
	defer log.WithField("worker", w).Info("web worker successfully closed")

	w.BaseWorker.Close()

	// Gracefully close WebSocket connection.
	err := w.conn.WriteControl(websocket.CloseMessage, make([]byte, 0), time.Now().Add(pongTimeout))
	if err != nil {
		log.WithField("worker", w).Error(err)
	}

	w.conn.Close()

	// Stop send and receive pumps.
	close(w.done)
	w.closing.Wait()
}

// sendPump repeatedly fetches messages piped from the master,
// and sends it to the worker via its WebSocket connection.
//
// It also periodcally pings a heartbeat message.
func (w *webWorker) sendPump() {
	w.closing.Add(1)
	defer w.closing.Done()

	defer close(w.Send)
	for {
		select {
		case <-w.done:
			return
		case <-time.Tick(pingPeriod):
			// Periodic keepalive ping.
			err := w.conn.WriteControl(websocket.PingMessage, make([]byte, 0), time.Now().Add(writeTimeout))
			if err != nil {
				log.WithField("worker", w).Error(err)
				w.CloseMe <- struct{}{}
				return
			}
		case message := <-w.Send:
			if err := w.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
				log.WithField("worker", w).Error(err)
				w.CloseMe <- struct{}{}
				return
			}

			if err := w.conn.WriteJSON(message); err != nil {
				log.WithField("worker", w).Error(err)
				w.CloseMe <- struct{}{}
				return
			}
		}
	}
}

// receivePump repeatedly reads messages from the WebSocket connection,
// and pipes them to the master.
func (w *webWorker) receivePump() {
	w.closing.Add(1)
	defer w.closing.Done()

	w.conn.SetReadLimit(maxMessageSize)

	// Set pong handling.
	w.conn.SetReadDeadline(time.Now().Add(pongTimeout))
	w.conn.SetPongHandler(func(string) error {
		err := w.conn.SetReadDeadline(time.Now().Add(pongTimeout))
		if err != nil {
			log.WithField("worker", w).Error(err)
			return err
		}
		return nil
	})

	defer close(w.Receive)

	// Keep reading until notified to close or on any error.
	recv := make(chan struct{})
	var message *protocol.Message
	var err error
	for {
		// Notify when a message has received.
		// This is necessary selecting from the network and the "close" channel together.
		go func() {
			err = w.conn.ReadJSON(message)
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
			w.Receive <- message
		}
	}
}
