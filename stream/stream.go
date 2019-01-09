package stream

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/pingcap/errors"

	"sseirc/splitter/streamid"
)

type StreamID = streamid.StreamID

type Stream struct {
	closedLock  sync.RWMutex
	closed      bool
	rdb         *redis.Client
	stream      string
	lastID      string
	messageLock sync.RWMutex
	messages    []redis.XMessage
	clientPing  chan struct{}
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
		//ensure that nothing raced us here
		if streams[stream] == nil {
			streams[stream] = &Stream{
				closed:     false,
				rdb:        rdb,
				stream:     stream,
				lastID:     "0-0",
				messages:   []redis.XMessage{},
				clientPing: make(chan struct{}, 1),
			}
			go streams[stream].monitor()
			go streams[stream].reaper()
		}
		streamLock.Unlock()

		streamLock.RLock()
	}

	return streams[stream]
}

func (s *Stream) Reopen() *Stream {
	log.Println("reopen")
	return New(s.rdb, s.stream)
}

func (s *Stream) Closed() bool {
	s.closedLock.RLock()
	defer s.closedLock.RUnlock()
	return s.closed
}

// ping alerts the reaper that this stream is not yet useless
func (s *Stream) ping() {
	s.clientPing <- struct{}{}
}

func (s *Stream) reaper() {
	for !s.Closed() {
		reapTime := time.NewTimer(30 * time.Second)
		select {
		case <-s.clientPing:
			reapTime.Stop()
			continue
		case <-reapTime.C:
			s.reap()
			return
		}
	}
}

func (s *Stream) reap() {
	s.closedLock.Lock()

	if s.closed {
		return
	}

	log.Println("reaping stream!", s.stream)
	s.closed = true

	streamLock.Lock()
	delete(streams, s.stream)
	streamLock.Unlock()

	s.closedLock.Unlock()

	s.messageLock.Lock()
	s.messages = nil
	s.messageLock.Unlock()
}

func (s *Stream) monitor() {
	defer log.Println("stream closed")
	for !s.Closed() {
		vals, err := s.rdb.XRead(&redis.XReadArgs{
			Streams: []string{s.stream, s.lastID},

			// limiting the total pulled with each iteration
			// allows us to avoid blocking other consumers
			Count: 2000,

			// a limited blocking time allows us to
			// discard disconnected clients periodically
			// in the absence of any new events
			Block: 1500,
		}).Result()

		if err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				// Block time elapsed
				continue
			}
			log.Println(err)
			time.Sleep(1 * time.Second)
		}

		if len(vals) != 1 {
			log.Printf("%+v\n", errors.New(fmt.Sprint("received an unexpected number of streams from XREAD", len(vals), s.stream)))
			s.reap()
			return
		}

		s.messageLock.Lock()
		msgs := vals[0].Messages
		s.lastID = msgs[len(msgs)-1].ID
		s.messages = append(s.messages, msgs...)
		s.messageLock.Unlock()
	}
}

var ErrTimeout = errors.New("stream read timeout")
var ErrClosed = errors.New("stream closed")

func (s *Stream) readInner(lastID StreamID) []redis.XMessage {
	s.messageLock.RLock()
	defer s.messageLock.RUnlock()

	if lastID == streamid.Zero && len(s.messages) > 0 {
		newMessages := append([]redis.XMessage{}, s.messages...)
		return newMessages
	}

	// naive algorithm, find our last sent message by
	// searching backwards until we either find it,
	// or until we find that we have passed it, which
	// would imply that our last is the very last message
	// in the queue even now and we need to wait.
	for index := len(s.messages) - 2; index >= 0; index-- {
		msgID := streamid.MustParse(s.messages[index].ID)
		if msgID.Less(lastID) {
			break
		}
		if msgID == lastID {
			newMessages := append([]redis.XMessage{}, s.messages[index+1:]...)
			return newMessages
		}
	}

	return nil
}

func (s *Stream) Read(lastID StreamID) ([]redis.XMessage, error) {
	deadline, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel() //in case we finish early

	for deadline.Err() == nil && !s.Closed() {
		s.ping() // keep the stream alive while we're interested
		newMessages := s.readInner(lastID)
		if newMessages != nil {
			log.Println(len(newMessages))
			return newMessages, nil
		}
		time.Sleep(50 * time.Millisecond)
	}

	if deadline.Err() != nil {
		return nil, ErrTimeout
	} else {
		return nil, ErrClosed
	}
}
