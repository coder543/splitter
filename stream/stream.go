package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
	"github.com/pingcap/errors"

	"sseirc/splitter/streamid"
)

type StreamID = streamid.StreamID

type Stream struct {
	closedLock    sync.RWMutex
	closed        bool
	rdb           *redis.Client
	stream        string
	lastID        string
	messages      []Message
	messageLock   sync.RWMutex
	messageSignal chan struct{}
	clients       int64
	reaper        *time.Timer
}

type Message struct {
	ID       StreamID
	StringID string
	Value    []byte
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
				closed:        false,
				rdb:           rdb,
				stream:        stream,
				lastID:        "0-0",
				messages:      []Message{},
				messageSignal: make(chan struct{}),
			}
			go streams[stream].monitor()
		}
		streamLock.Unlock()

		streamLock.RLock()
	}

	return streams[stream]
}

func (s *Stream) Reopen() *Stream {
	return New(s.rdb, s.stream)
}

func (s *Stream) Closed() bool {
	s.closedLock.RLock()
	defer s.closedLock.RUnlock()
	return s.closed
}

func (s *Stream) waitChannel() <-chan struct{} {
	s.messageLock.RLock()
	defer s.messageLock.RUnlock()
	return s.messageSignal
}

func (s *Stream) waiting() {
	atomic.AddInt64(&s.clients, 1)
	s.closedLock.RLock()
	if s.reaper != nil {
		s.closedLock.RUnlock()
		s.closedLock.Lock()
		// ensure no one raced us here
		if s.reaper != nil {
			s.reaper.Stop()
			s.reaper = nil
		}
		s.closedLock.Unlock()
		return
	}
	s.closedLock.RUnlock()
}

func (s *Stream) done() {
	if atomic.AddInt64(&s.clients, -1) == 0 {
		s.closedLock.Lock()
		s.reaper = time.AfterFunc(30*time.Second, s.reap)
		s.closedLock.Unlock()
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
	close(s.messageSignal) // awaken all sleeping clients
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
			// exit cleanly if the Stream is closed
			Block: 15000,
		}).Result()

		if s.Closed() {
			// avoid saving any new messages if we're closed
			return
		}

		if err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				// blocking time elapsed
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
		for _, msg := range msgs {
			value, err := json.Marshal(msg.Values)
			if err != nil {
				log.Printf("%+v\n", errors.Annotate(err, "failed to parse msg"))
				s.reap()
				return
			}
			s.messages = append(s.messages, Message{
				ID:       streamid.MustParse(msg.ID),
				StringID: msg.ID,
				Value:    value,
			})
		}
		close(s.messageSignal)                // signal all waiting clients
		s.messageSignal = make(chan struct{}) // create new signal channel
		s.messageLock.Unlock()
	}
}

var ErrTimeout = errors.New("stream read timeout")
var ErrClosed = errors.New("stream closed")

var maxMessages = 2000

func (s *Stream) readInner(lastID StreamID) []Message {
	s.messageLock.RLock()
	defer s.messageLock.RUnlock()

	if lastID == streamid.Zero && len(s.messages) > 0 {
		var newMessages []Message
		// keep from letting peak memory usage be too high
		if len(s.messages) > maxMessages {
			newMessages = append(newMessages, s.messages[:maxMessages]...)
		} else {
			newMessages = append(newMessages, s.messages...)
		}
		return newMessages
	}

	newStart := 1 + sort.Search(len(s.messages), func(index int) bool {
		msg := s.messages[index]
		return msg.ID == lastID || lastID.Less(msg.ID)
	})

	// if this isn't the very last index or no result at all
	if newStart < len(s.messages) {
		stop := len(s.messages)
		if stop-newStart > maxMessages {
			stop = newStart + maxMessages
		}
		newMessages := append([]Message{}, s.messages[newStart:stop]...)
		return newMessages
	}

	return nil
}

func (s *Stream) Read(ctx context.Context, lastID StreamID) ([]Message, error) {
	s.waiting()    // keep the stream alive while we're interested
	defer s.done() // but let it know when we're done here

	for ctx.Err() == nil && !s.Closed() {
		newMessages := s.readInner(lastID)
		if newMessages != nil {
			return newMessages, nil
		}
		select {
		case <-s.waitChannel():
			continue
		case <-ctx.Done():
			break
		}
	}

	if ctx.Err() != nil {
		return nil, ErrTimeout
	} else {
		return nil, ErrClosed
	}
}
