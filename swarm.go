package swarm

import (
	"encoding/json"
	"github.com/fzzy/radix/redis"
	"log"
	"runtime"
	"time"
)

var (
	handlerMap map[string]Handler
)

type Master struct {
	addr  string
	queue string
}

type Job struct {
	Name string
	Args []interface{}
	TTL  int
}

type Handler interface {
	Handle(...interface{})
}

type HandlerFunc func(...interface{})

func (f HandlerFunc) Handle(args ...interface{}) {
	f(args...)
}

func NewMaster(addr, queue string) *Master {
	return &Master{addr: addr, queue: queue}
}

func (m *Master) Submit(name string, args ...interface{}) {
	redis_conn, err := redis.Dial("tcp", m.addr)
	if err != nil {
		log.Fatalf("Can't connect to redis@%s: %s", m.addr, err)
	}
	defer redis_conn.Close()
	job := Job{
		Name: name,
		Args: args,
		TTL:  0,
	}
	jobBlob, _ := json.Marshal(job)
	redis_conn.Cmd("LPUSH", m.queue, string(jobBlob))
}

func Handle(jobName string, h Handler) {
	if handlerMap == nil {
		handlerMap = make(map[string]Handler, 0)
	}
	handlerMap[jobName] = h
}

func Listen(addr, queue string, workers int) {
	redis_conn, err := redis.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("Can't connect to master@%s: %s", addr, err)
	}
	defer redis_conn.Close()
	freeWorkersChan := make(chan string, workers)
	for i := 0; i < workers; i++ {
		workerId := GenerateId() 
		freeWorkersChan <- workerId
	}

	runtime.GOMAXPROCS(workers)
	for workerId := range freeWorkersChan {
		job := Job{}
		reply, err := redis_conn.Cmd("BRPOP", queue, 0).List()
		if err != nil {
			log.Printf("Can't receive job message:", err)
			freeWorkersChan <- workerId
			continue
		}
		err = json.Unmarshal([]byte(reply[1]), &job)
		if err != nil {
			log.Printf("Can't parse job message:", err)
			freeWorkersChan <- workerId
			continue
		}
		handler, found := handlerMap[job.Name]
		if !found {
			log.Printf("Cannot process job %s", job.Name)
			job.TTL -= 1
			if job.TTL > 0 {
				jobBlob, _ := json.Marshal(job)
				redis_conn.Cmd("PUSH", queue, string(jobBlob))
			}
			freeWorkersChan <- workerId
			continue
		}
		go func(workerId string, job Job) {
			log.Printf("[%s] Received job '%s' with args '%v'", workerId, job.Name, job.Args)
			start_time := time.Now()
			handler.Handle(job.Args...)
			log.Printf("[%s] Job %s complete in %s", workerId, job.Name, time.Since(start_time))
			freeWorkersChan <- workerId
		}(workerId, job)
	}
}
