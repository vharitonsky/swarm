package main

import (
	"flag"
	"github.com/vharitonsky/swarm"
	"log"
	"runtime"
)

var (
	workers = flag.Int("workers", 1, "Number of workers(threads) in a process.")
	master  = flag.String("master", ":6379", "Addr of master redis to listen to.")
	queue   = flag.String("queue", "default", "Name of queue to listen to")
)

func EchoHandler(args ...interface{}) {
	log.Print(args)
}

func main() {
	flag.Parse()
	log.Print("Running EchoHandler. Master:", *master)
	runtime.GOMAXPROCS(*workers)
	swarmMaster := swarm.NewMaster(*master, *queue)
	swarmMaster.Submit("echo", []interface{}{1, 2, 3}...)
	swarm.Handle("echo", swarm.HandlerFunc(EchoHandler))
	swarm.Listen(*master, *queue, *workers)
}
