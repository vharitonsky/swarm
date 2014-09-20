package swarm

import (
	"encoding/json"
	"fmt"
	"github.com/fzzy/radix/redis"
	"log"
	"math"
	"os"
	"time"
)

var (
	handlerMap map[string]Handler
)

type Master struct {
	addr  string
	queue string
}

type Slave struct {
	id             string
	addr           string
	queue          string
	pool           chan string
	workersCount   int
	busyWorkersMap map[string]bool
}

type Job struct {
	Name string
	Args []interface{}
	TTL  int
}

type SlaveReport struct {
	Id           string
	BusyWorkers  int
	TotalWorkers int
	Hostname     string
	ListeningTo  string
	Generated    string
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

func NewSlave(addr, queue string, workersCount int) *Slave {
	slave := Slave{
		id:             GenerateId(),
		addr:           addr,
		queue:          queue,
		workersCount:   workersCount,
		pool:           make(chan string, workersCount),
		busyWorkersMap: make(map[string]bool, 0),
	}
	for i := 0; i < workersCount; i++ {
		slave.pool <- GenerateId()
	}
	return &slave
}

func (s *Slave) allocWorker(workerId string) {
	s.busyWorkersMap[workerId] = true
}

func (s *Slave) freeWorker(workerId string) {
	delete(s.busyWorkersMap, workerId)
}

func (s *Slave) Serve() (err error) {
	redisConn, err := redis.Dial("tcp", s.addr)
	if err != nil {
		err = fmt.Errorf("Can't connect to master@%s: %s", s.addr, err)
		return
	}
	defer redisConn.Close()
	go func() {
		for {
			s.SubmitReport()
			time.Sleep(30 * time.Second)
		}
	}()
	for workerId := range s.pool {
		job := Job{}
		reply, err := redisConn.Cmd("BRPOP", s.queue, 0).List()
		if err != nil {
			return fmt.Errorf("Can't receive job message:", err)
		}
		err = json.Unmarshal([]byte(reply[1]), &job)
		if err != nil {
			log.Printf("Can't parse job message:", err)
			s.pool <- workerId
			continue
		}
		handler, found := handlerMap[job.Name]
		if !found {
			log.Printf("Cannot process job %s", job.Name)
			job.TTL -= 1
			if job.TTL > 0 {
				jobBlob, _ := json.Marshal(job)
				redisConn.Cmd("PUSH", s.queue, string(jobBlob))
			}
			s.pool <- workerId
			continue
		}
		go func(workerId string, job Job) {
			log.Printf("[%s] Received job '%s' with args '%v'", workerId, job.Name, job.Args)
			s.allocWorker(workerId)
			defer s.freeWorker(workerId)
			start_time := time.Now()
			handler.Handle(job.Args...)
			log.Printf("[%s] Job '%s' with args '%v' complete in %s", workerId, job.Name, job.Args, time.Since(start_time))
			s.pool <- workerId
		}(workerId, job)
	}
	return nil
}

func (s *Slave) GetReport() SlaveReport {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Can't get hostname: %s", err)
	}
	return SlaveReport{
		Id:           s.id,
		TotalWorkers: s.workersCount,
		BusyWorkers:  s.workersCount - len(s.busyWorkersMap),
		ListeningTo:  s.queue,
		Hostname:     hostname,
		Generated:    time.Now().Format(time.UnixDate),
	}
}

func (s *Slave) SubmitReport() {
	redisConn, err := redis.Dial("tcp", s.addr)
	if err != nil {
		err = fmt.Errorf("Can't connect to master to submit report@%s: %s", s.addr, err)
		return
	}
	defer redisConn.Close()
	report := s.GetReport()
	slaveReport, err := json.Marshal(report)
	redisConn.Cmd("HSET", "_swarm_worker_reports", s.id, string(slaveReport))
}

func (m *Master) Submit(name string, args ...interface{}) {
	redisConn, err := redis.Dial("tcp", m.addr)
	if err != nil {
		log.Fatalf("Can't connect to redis@%s: %s", m.addr, err)
	}
	defer redisConn.Close()
	job := Job{
		Name: name,
		Args: args,
		TTL:  0,
	}
	jobBlob, _ := json.Marshal(job)
	redisConn.Cmd("LPUSH", m.queue, string(jobBlob))
}

func Handle(jobName string, h Handler) {
	if handlerMap == nil {
		handlerMap = make(map[string]Handler, 0)
	}
	handlerMap[jobName] = h
}

func (m *Master) WatchSlaves() {
	for {
		time.Sleep(30 * time.Second)
		redisConn, err := redis.Dial("tcp", m.addr)
		if err != nil {
			log.Fatalf("Can't connect to master@%s: %s", m.addr, err)
		}
		defer redisConn.Close()
		reply, err := redisConn.Cmd("HGETALL", "_swarm_worker_reports").List()
		if err != nil {
			log.Fatalf("Can't get reply from master@%s: %s", m.addr, err)
		}
		if len(reply) == 0 {
			log.Print("No running slaves")
			continue
		}
		var lastkey string

		runningReports := make([]string, 0)

		for i, value := range reply {
			if math.Mod(float64(i+1), 2) != 0 {
				lastkey = value
				continue
			}
			report := SlaveReport{}
			err = json.Unmarshal([]byte(value), &report)
			if err != nil {
				log.Fatalf("Can't parse worker report: %s", err)
			}
			reportGenerationTime, err := time.Parse(time.UnixDate, report.Generated)
			if err != nil || time.Since(reportGenerationTime) > 1*time.Minute {
				redisConn.Cmd("HDEL", "_swarm_worker_reports", lastkey)
				continue
			}
			runningReports = append(runningReports, fmt.Sprintf("\n"+
				"ID: %s\n"+
				"HOST: %s\n"+
				"WORKERS: %d/%d\n"+
				"QUEUES: %s\n"+
				"LAST SEEN: %s\n\n",
				report.Id, report.Hostname, report.TotalWorkers, report.BusyWorkers, report.ListeningTo, report.Generated))
		}
		if len(runningReports) == 0 {
			log.Print("No running slaves")
			continue
		}
		log.Print("====START SLAVE REPORT====")
		for _, report := range runningReports {
			fmt.Print(report)
		}
		log.Print("==== END SLAVE REPORT ====")
	}
}

func Listen(addr, queue string, workersCount int) error {
	return NewSlave(addr, queue, workersCount).Serve()
}
