package streamid

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
)

type StreamID struct {
	time int64
	seq  int64
}

func Less(a, b StreamID) bool {
	return a.time < b.time || (a.time == b.time && a.seq < b.seq)
}

func Parse(raw string) (StreamID, error) {
	parts := strings.Split(strings.TrimSpace(raw), "-")
	if len(parts) != 2 {
		return StreamID{}, errors.New(fmt.Sprint("invalid StreamID:", raw))
	}

	time, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return StreamID{}, errors.Annotatef(err, "invalid StreamID time: %s", parts[0])
	}

	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return StreamID{}, errors.Annotatef(err, "invalid StreamID seq: %s", parts[1])
	}

	return StreamID{time, seq}, nil
}

func MustParse(raw string) StreamID {
	id, err := Parse(raw)
	if err != nil {
		log.Panicln(err)
	}
	return id
}

func (s StreamID) String() string {
	return fmt.Sprintf("%d-%d", s.time, s.seq)
}
