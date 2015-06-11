// Package main implements an example web-mapreduce server, capable of managing multiple masters.
package main

import (
	"net/http"
	"os"
	"os/signal"
	"path"
	"runtime"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/contrib/ginrus"
	"github.com/gin-gonic/gin"
	"github.com/rounds/gin-cors"
)

const (
	ginMode = "debug"
	Addr    = "0.0.0.0:80"
)

func init() {
	gin.DisableBindValidation() // Disable automatic validation of requests' payload.
	gin.SetMode(ginMode)        // debug, test, or release.

	// Set GOMAXPROCS to use all available CPUs.
	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)
	log.WithField("GOMAXPROCS", cpus).Info("GOMAXPROCS set")
}

// InitServer returns a ready-to-run HTTP server.
//
// It uses Gin super-fast web framework.
func InitServer() *http.Server {
	engine := gin.New()
	engine.Use(
		gin.Recovery(),
		ginrus.Ginrus(log.StandardLogger(), time.RFC3339, true),
	)

	// CORS support
	cors := cors.Middleware(cors.Options{
		AllowOrigins:     []string{"*"},
		AllowCredentials: false,
		AllowMethods:     nil,        // Use default (see defaultAllowMethods).
		AllowHeaders:     []string{}, // Copy from Access-Control-Allow-Headers.
		ExposeHeaders:    []string{},
		MaxAge:           10 * time.Minute,
	})
	engine.Use(cors)
	engine.OPTIONS("/*cors", cors)

	// Static files and templates.
	_, filepath, _, _ := runtime.Caller(1)
	base := path.Dir(filepath)
	engine.LoadHTMLGlob(path.Join(base, "static/templates/*"))
	engine.Static("/css", path.Join(base, "static/css"))
	engine.Static("/js", path.Join(base, "static/js"))

	// Routes
	engine.GET("/", Index)
	engine.POST("/algorithm", NewAlgorithm)
	engine.GET("/worker/*prefix", NewWorker)

	server := http.Server{
		Addr:    Addr,
		Handler: engine,
	}

	return &server
}

func main() {
	defer func() {
		close(done)
		closing.Wait()

		log.Warn("goodbye")
		os.Exit(0)
	}()

	// Start HTTP server.
	server := InitServer()
	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Error(err)
		}
	}()

	// Wait for terminating signal.
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGHUP)
	log.WithField("signal", <-sigc).Warn("caught signal")
}
