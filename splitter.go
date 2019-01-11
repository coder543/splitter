package splitter

import (
	"log"
	"net/http"
	"sseirc/splitter/client"
	"sseirc/splitter/stream"
	"sseirc/splitter/streamid"
	"time"

	"github.com/go-redis/redis"
)

type StreamID = streamid.StreamID

func Handle(w http.ResponseWriter, r *http.Request, rdb *redis.Client, streamPath string) {
	client := client.New(w, r, rdb, streamPath)
	defer client.Shutdown()

	ctx := r.Context()

	// until the client disconnects, keep sending them updates
	for client.Connected() {
		vals, err := client.Read(ctx)

		if err == stream.ErrTimeout {
			continue
		} else if err == stream.ErrClosed {
			// Something is wrong with Redis.
			// Log it and then try again in 10 seconds.
			log.Println("Redis Err", err)
			blocked, err := client.Info("status", "degraded")
			if blocked || err != nil {
				log.Println("dropping client that can't be informed")
				return
			}
			time.Sleep(10 * time.Second)
			continue
		} else if err != nil {
			log.Panicf("unknown err %+v\n", err)
		}

		for _, message := range vals {
			slowClient := 0
			for {
				blocked, err := client.Send(message)
				if err != nil {
					log.Panicf("%+v", err)
				}
				if blocked && slowClient < 40 {
					slowClient += 1
					time.Sleep(time.Duration(slowClient * int(time.Millisecond)))
				} else if blocked {
					log.Println("dropping slow client")
					return
				} else {
					break
				}
			}
		}

	}
}
