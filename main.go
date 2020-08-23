package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var limiter <-chan time.Time = time.Tick(3 * time.Millisecond)
var counter uint64

type fs2DBArgs struct {
	ID       string
	Filename string
	Index    string
}

var fs2DBQueue chan fs2DBArgs
var finish <-chan int
var wg sync.WaitGroup

func ElasticLoop() {
	for {
		<-limiter
		args := <-fs2DBQueue
		go doElasticPOSTRequest(args.ID, args.Filename, args.Index)
	}
}

func doElasticPOSTRequest(_id string, filename string, index string) error {
	var err error

	file, err := os.Open(filename)
	if err != nil {
		log.Println(err)
		atomic.AddUint64(&counter, 1)
		wg.Done()
	}
	defer file.Close()
	resp, err := http.Post("http://localhost:9200/"+index+"/_doc/"+_id+"/?pretty", "application/json", file)
	if err != nil {
		log.Println(err)
		atomic.AddUint64(&counter, 1)
		wg.Done()
		return nil
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		atomic.AddUint64(&counter, 1)
		wg.Done()
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		log.Println(string(body))
		atomic.AddUint64(&counter, 1)
		wg.Done()
		return nil
	}
	atomic.AddUint64(&counter, 1)
	if counter%1000 == 0 {
		log.Println("Elastic POSTs Counter", counter)
	}
	wg.Done()
	return nil
}

func populateElasticDB() error {
	var err error
	groupsDirList, err := ioutil.ReadDir(filepath.Join(".data", "comments"))
	if err != nil {
		return err
	}
	// for group in groups
	for _, groupFileInfo := range groupsDirList {
		if groupFileInfo.IsDir() {
			postsDirList, err := ioutil.ReadDir(filepath.Join(".data", "comments", groupFileInfo.Name()))
			if err != nil {
				return err
			}
			for _, postFileInfo := range postsDirList {
				if postFileInfo.IsDir() {
					commentsDirList, err := ioutil.ReadDir(filepath.Join(".data", "comments", groupFileInfo.Name(), postFileInfo.Name()))
					if err != nil {
						return err
					}
					// for comment in post.comments
					for _, commentFileInfo := range commentsDirList {
						if !commentFileInfo.IsDir() {
							filename := filepath.Join(".data", "comments", groupFileInfo.Name(), postFileInfo.Name(), commentFileInfo.Name())
							_id := strings.Split(groupFileInfo.Name()+"_"+postFileInfo.Name()+"_"+commentFileInfo.Name(), ".json")
							wg.Add(1)
							fs2DBQueue <- fs2DBArgs{
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

func main() {
	fs2DBQueue = make(chan fs2DBArgs)
	go ElasticLoop()

	err := populateElasticDB()
	if err != nil {
		panic(err)
	}

	wg.Wait()
}
