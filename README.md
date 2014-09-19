SWARM
=====

Distributed job execution library

Swarm is a distributed multithreaded job execution system tightly coupled with Redis.
It was intended as an easy drop-in solution for IO or processor heavy jobs which can be easyly
parallelized.

To use Swarm all you need a Redis server reacheable across your local network. Redis is used as a
transport and a broker at the same time.

Example slave code:

```go

package main

import (
	"github.com/vharitonsky/swarm"
	"log"
)

func EchoHandler(args ...interface{}) {
	log.Print(args)
}

func main() {
	masterAddr := ":6379"
	queue := "default"
	workerCount := 2
	swarm.Handle("echo", swarm.HandlerFunc(EchoHandler))
	swarm.Listen(masterAddr, queue, workerCount)
}

```

This code launches a slave node which will listen to masterAddr at :6379 which is a local default port of redis,
 "default" queue and will have 2 workers in a pool. 

Example master code:

```go

package main

import (
	"github.com/vharitonsky/swarm"
)

func main() {
	masterAddr := ":6379"
	queue := "default"
	swarmMaster := swarm.NewMaster(masterAddr, queue)
	swarmMaster.Submit("echo", []interface{}{1, 2, 3}...)
	swarmMaster.WatchSlaves()
}



```

This code submits a task named "echo" to default queue at :6379. 
Then it starts a watch slaves cycle which will report slave status of all slaves connected to masterAddr.


 Go does not multithread by default, so if you want to run your tasks
 across multiple cores you have to explicitly tell it to:

```go

import runtime

runtime.GOMAXPROCS(runtime.NumCPU())

```

 You have to run this before starting Listen routine. This will allow Swarm to run your tasks on all cores.

