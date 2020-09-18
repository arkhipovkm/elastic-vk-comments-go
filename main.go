package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
)

var counter uint64
var ELASTIC_IP_ADDR string
var DATA_DIR string

func dumpCounters() error {
	var filename string
	var err error

	filename = filepath.Join("counter.txt")
	err = ioutil.WriteFile(filename, []byte(strconv.Itoa(int(counter))), os.ModePerm)
	if err != nil {
		return err
	}
	return err
}

func loadCounters() error {
	var err error
	filename := filepath.Join("counter.txt")
	body, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	cnt, _ := strconv.Atoi(string(body))
	counter += uint64(cnt)

	return err
}

func doElasticPOSTRequest(_id string, filename string, index string) error {
	var err error

	file, err := os.Open(filename)
	if err != nil {
		log.Println(err)
		atomic.AddUint64(&counter, 1)
		return err
	}
	defer file.Close()
	resp, err := http.Post("http://"+ELASTIC_IP_ADDR+":9200/"+index+"/_doc/"+_id+"/?pretty", "application/json", file)
	if err != nil {
		log.Println(err)
		atomic.AddUint64(&counter, 1)
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		atomic.AddUint64(&counter, 1)
		return err
	}
	// bodyStr := string(body)
	// log.Println(bodyStr)
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		log.Println(string(body))
		atomic.AddUint64(&counter, 1)
		return err
	}
	atomic.AddUint64(&counter, 1)
	if counter%1000 == 0 {
		log.Println("Elastic POSTs Counter", counter)
	}
	return err
}

type WorkRequest struct {
	ID       string
	Filename string
	Index    string
}

type Worker struct {
	ID          int
	Work        chan WorkRequest
	WorkerQueue chan chan WorkRequest
	QuitChan    chan bool
}

// A buffered channel that we can send work requests on.
var WorkQueue = make(chan WorkRequest)
var WorkerQueue chan chan WorkRequest

// NewWorker creates, and returns a new Worker object. Its only argument
// is a channel that the worker can add itself to whenever it is done its
// work.
func NewWorker(id int, workerQueue chan chan WorkRequest) Worker {
	// Create, and return the worker.
	worker := Worker{
		ID:          id,
		Work:        make(chan WorkRequest),
		WorkerQueue: workerQueue,
		QuitChan:    make(chan bool)}

	return worker
}

// This function "starts" the worker by starting a goroutine, that is
// an infinite "for-select" loop.
func (w *Worker) Start() {
	go func() {
		for {
			// Add ourselves into the worker queue.
			w.WorkerQueue <- w.Work

			select {
			case work := <-w.Work:
				// Receive a work request.
				doElasticPOSTRequest(work.ID, work.Filename, work.Index)
			case <-w.QuitChan:
				// We have been asked to stop.
				fmt.Printf("worker%d stopping\n", w.ID)
				return
			}
		}
	}()
}

// Stop tells the worker to stop listening for work requests.
//
// Note that the worker will only stop *after* it has finished its work.
func (w *Worker) Stop() {
	go func() {
		w.QuitChan <- true
	}()
}

func StartDispatcher(nworkers int) {
	// First, initialize the channel we are going to but the workers' work channels into.
	WorkerQueue = make(chan chan WorkRequest, nworkers)

	// Now, create all of our workers.
	for i := 0; i < nworkers; i++ {
		fmt.Println("Starting worker", i+1)
		worker := NewWorker(i+1, WorkerQueue)
		worker.Start()
	}

	go func() {
		for {
			select {
			case worker := <-WorkerQueue:
				work := <-WorkQueue
				worker <- work
			}
		}
	}()
}

func Collector() error {
	var err error
	var i uint64
	groupsDirList, err := ioutil.ReadDir(filepath.Join(DATA_DIR, "comments"))
	if err != nil {
		return err
	}
	// for group in groups
	for _, groupFileInfo := range groupsDirList {
		if groupFileInfo.IsDir() {
			postsDirList, err := ioutil.ReadDir(filepath.Join(DATA_DIR, "comments", groupFileInfo.Name()))
			if err != nil {
				return err
			}
			for _, postFileInfo := range postsDirList {
				if postFileInfo.IsDir() {
					commentsDirList, err := ioutil.ReadDir(filepath.Join(DATA_DIR, "comments", groupFileInfo.Name(), postFileInfo.Name()))
					if err != nil {
						return err
					}
					i += uint64(len(commentsDirList))
					// log.Println(i)
					if i < counter {
						continue
					}
					// for comment in post.comments
					for _, commentFileInfo := range commentsDirList {
						if !commentFileInfo.IsDir() {
							filename := filepath.Join(DATA_DIR, "comments", groupFileInfo.Name(), postFileInfo.Name(), commentFileInfo.Name())
							_id := strings.Split(groupFileInfo.Name()+"_"+postFileInfo.Name()+"_"+commentFileInfo.Name(), ".json")
							WorkQueue <- WorkRequest{
								ID:       _id[0],
								Filename: filename,
								Index:    "comments-1",
							}
							if err != nil {
								return err
							}
						}
					}
				}
			}
		}
	}
	return err
}

func handleInterrupt() {
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sigCh:
		dumpCounters()
		log.Println("Exiting gracefully..")
		os.Exit(0)
	}
}

func main() {
	dataDirFlag := flag.String("d", "", "Data Directory")
	elasticHostnameFlag := flag.String("h", "", "IP address or hostname of the ElasticSearch Server")
	flag.Parse()

	DATA_DIR = *dataDirFlag
	ELASTIC_IP_ADDR = *elasticHostnameFlag

	if DATA_DIR == "" {
		panic(errors.New("No Data Dir provided"))
	}

	if ELASTIC_IP_ADDR == "" {
		panic(errors.New("No Elastic IP address provided"))
	}

	log.Println(DATA_DIR)
	log.Println(ELASTIC_IP_ADDR)

	loadCounters()
	log.Println("Loaded counter", counter)
	defer dumpCounters()
	go handleInterrupt()

	dumpCounters()
	StartDispatcher(30)
	go Collector()
	select {}
}
