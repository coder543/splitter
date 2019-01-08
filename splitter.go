package splitter

import (
	"log"
	"net"
	"net/http"
	"time"

	"github.com/go-redis/redis"

	"sseirc/splitter/client"
	"sseirc/splitter/streamid"
)

type StreamID = streamid.StreamID

func Handle(w http.ResponseWriter, r *http.Request, rdb *redis.Client, stream string) {
	client := client.New(w, r, rdb, stream)
	defer client.Shutdown()

	// until the client disconnects, keep sending them updates
	for client.Connected() {
		vals, err := client.Read()

		if err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				// Block time elapsed
				continue
			}

			// Something is wrong with Redis.
			// Log it and then try again in 10 seconds.
			log.Println(err)
			blocked, err := client.Info("status", "degraded")
			if blocked || err != nil {
				log.Println("dropping client that can't be informed")
				return
			}
			time.Sleep(10 * time.Second)
			continue
		}

		for _, message := range vals {
			slowClient := 0
			for {
				blocked, err := client.Send(message.ID, message.Values)
				if err != nil {
					log.Panicf("%+v", err)
				}
				if blocked && slowClient < 5 {
					slowClient += 1
					time.Sleep(25 * time.Millisecond)
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
