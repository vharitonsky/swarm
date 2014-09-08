package swarm

import(
	"flag"
	"runtime"
	"github.com/fzzy/radix/redis"
)

var (
	workers = flag.Int("workers", 1, "Number of workers(threads) in a process.")
	master = flag.String("master", ":6379", "Addr of master redis to listen to.")
	

	redis_conn = *redis.Client
)

func init(){
	runtime.GOMAXPROCS(*workers)
	redis_conn, err := redis.Dial("tcp", *redis_addr)
	if err != nil {
		log.Fatalf("Can't connect to master@%s: %s", *master, err)
	}
}

func Handler(){}

func HandlerFunc(){}

func Listen(){}