package main

import (
	"flag"
	"github.com/vharitonsky/swarm"
	"log"
)

var (
	workers = flag.Int("workers", 1, "Number of workers(threads) in a process.")
	master  = flag.String("master", ":6379", "Addr of master redis to listen to.")
	queue   = flag.String("queue", "default", "Name of queue to listen to")
)

func main() {
	flag.Parse()
	log.Print("Running slave watcher. Master:", *master)
	swarmMaster := swarm.NewMaster(*master, *queue)
	swarmMaster.WatchSlaves()
}
