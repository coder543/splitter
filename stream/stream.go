package stream

import (
	"fmt"

	"github.com/pingcap/errors"

	"github.com/go-redis/redis"

	"sseirc/splitter/streamid"
)

type StreamID = streamid.StreamID

type Stream struct {
	rdb    *redis.Client
	stream string
}

func New(rdb *redis.Client, stream string) Stream {
	return Stream{rdb, stream}
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
