package client

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/coder543/eventsource/v3"
	"github.com/go-redis/redis"
	"github.com/pingcap/errors"

	"sseirc/splitter/stream"
	"sseirc/splitter/streamid"
)

type StreamID = streamid.StreamID

type Client struct {
	lastID StreamID
	sse    *eventsource.Client
	ctx    context.Context
	stream stream.Stream
}

func New(w http.ResponseWriter, r *http.Request, rdb *redis.Client, streamPath string) *Client {
	sse := eventsource.NewClient(w, r)
	if sse == nil {
		http.Error(w, "could not create SSE writer", http.StatusInternalServerError)
		return nil
	}

	ctx := r.Context()

	lastIDRaw := r.Header.Get("Last-Event-ID")
	if lastIDRaw == "" {
		lastIDRaw = r.URL.Query().Get("lastEventId")
		if lastIDRaw == "" {
			lastIDRaw = "0-0"
		}
	}

	lastID, err := streamid.Parse(lastIDRaw)
	if err != nil {
		http.Error(w, "invalid lastEventID!", http.StatusBadRequest)
		return nil
	}

	return &Client{lastID: lastID, sse: sse, ctx: ctx, stream: stream.New(rdb, streamPath)}
}

func (c Client) Shutdown() {
	c.sse.Shutdown()
}

func (c Client) Connected() bool {
	return c.ctx.Err() == nil
}

// Send returns whether it was blocked, and whether an error occured
func (c *Client) Send(rawID string, message map[string]interface{}) (bool, error) {
	newID, err := streamid.Parse(rawID)
	if err != nil {
		return false, errors.AddStack(err)
	}

	ev := &eventsource.Event{}
	ev.ID(rawID)

	err = json.NewEncoder(ev).Encode(message)
	if err != nil {
		return false, errors.AddStack(err)
	}

	c.lastID = newID

	blocked, _ := c.sse.SendNonBlocking(ev)

	return blocked, nil
}

// Send returns whether it was blocked, and whether an error occured
func (c *Client) Info(key, value string) (bool, error) {
	ev := &eventsource.Event{}

	ev.ID(c.lastID.String())
	ev.Type("info")

	err := json.NewEncoder(ev).Encode(map[string]string{key: value})
	if err != nil {
		return false, errors.AddStack(err)
	}

	blocked, _ := c.sse.SendNonBlocking(ev)

	return blocked, nil
}

func (c *Client) Read() ([]redis.XMessage, error) {
	return c.stream.Read(c.lastID)
}
