package swarm

import (
	"encoding/json"
	"github.com/fzzy/radix/redis"
	"log"
	"runtime"
)

type Job struct {
	Name string
	Args interface{}
	TTL  int
}

type Handler interface {
	Handle()
}

func HandlerFunc() {}

func Submit(addr, queue, name string, args interface{}) {
	redis_conn, err := redis.Dial("tcp", addr)
	defer redis_conn.Close()
	job := Job{
		Name: name,
		Args: args,
		TTL:  0,
	}
	jobBlob, _ := json.Marshal(job)
	redis_conn.Cmd("PUSH", queue, string(jobBlob))
}

func Listen(addr, queue string, workers int) {
	redis_conn, err := redis.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("Can't connect to master@%s: %s", addr, err)
	}
	defer redis_conn.Close()
	handlerMap := make(map[string]*Handler, 0)
	freeWorkersChan := make(chan bool, workers)
	for i := 0; i < workers; i++ {
		freeWorkersChan <- true
	}

	runtime.GOMAXPROCS(workers)
	for _ = range freeWorkersChan {
		job := Job{}
		reply, err := redis_conn.Cmd("BRPOP", queue).Bytes()
		err = json.Unmarshal(reply, &job)
		if err != nil {
			log.Printf("Can't receive job message:", err)
			freeWorkersChan <- true
		}
		handler, found := handlerMap[job.Name]
		if !found {
			log.Printf("Cannot process job %s", job.Name)
			job.TTL -= 1
			if job.TTL > 0 {
				jobBlob, _ := json.Marshal(job)
				redis_conn.Cmd("PUSH", queue, string(jobBlob))
			}
			freeWorkersChan <- true
			continue
		}
		go func() {
			handler.Handle(job.Args)
			freeWorkersChan <- true
		}()
	}
}
