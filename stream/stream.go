package stream

import (
	"fmt"
	"sync"

	"github.com/go-redis/redis"
	"github.com/pingcap/errors"

	"sseirc/splitter/streamid"
)

type StreamID = streamid.StreamID

type Stream struct {
	rdb    *redis.Client
	stream string
}

var streamLock sync.RWMutex
var streams map[string]*Stream = map[string]*Stream{}

func New(rdb *redis.Client, stream string) *Stream {
	streamLock.RLock()
	defer streamLock.RUnlock()

	// if it doesn't exist yet, create it
	if streams[stream] == nil {
		streamLock.RUnlock()

		streamLock.Lock()
		streams[stream] = &Stream{rdb, stream}
		streamLock.Unlock()

		streamLock.RLock()
	}

	return streams[stream]
}

func (s *Stream) Read(lastID StreamID) ([]redis.XMessage, error) {
	vals, err := s.rdb.XRead(&redis.XReadArgs{
		Streams: []string{s.stream, lastID.String()},

		// limiting the total pulled with each iteration
		// allows us to avoid blocking other consumers
		Count: 2000,

		// a limited blocking time allows us to
		// discard disconnected clients periodically
		// in the absence of any new events
		Block: 1500,
	}).Result()

	if err != nil {
		return nil, err
	}

	if len(vals) != 1 {
		return nil, errors.New(fmt.Sprint("received an unexpected number of streams from XREAD", len(vals)))
	}

	return vals[0].Messages, nil
}
