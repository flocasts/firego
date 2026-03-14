package firego

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// EventTypePut is the event type sent when new data is inserted to the
	// Firebase instance.
	EventTypePut = "put"
	// EventTypePatch is the event type sent when data at the Firebase instance is
	// updated.
	EventTypePatch = "patch"
	// EventTypeError is the event type sent when an unknown error is encountered.
	EventTypeError = "event_error"
	// EventTypeAuthRevoked is the event type sent when the supplied auth parameter
	// is no longer valid.
	EventTypeAuthRevoked = "auth_revoked"

	eventTypeKeepAlive  = "keep-alive"
	eventTypeCancel     = "cancel"
	eventTypeRulesDebug = "rules_debug"
)

// Event represents a notification received when watching a
// firebase reference.
type Event struct {
	// Type of event that was received
	Type string
	// Path to the data that changed
	Path string
	// Data that changed
	Data interface{}

	rawData []byte
}

// Value converts the raw payload of the event into the given interface.
func (e Event) Value(v interface{}) error {
	var tmp struct {
		Data interface{} `json:"data"`
	}
	tmp.Data = &v
	return json.Unmarshal(e.rawData, &tmp)
}

// StopWatching stops tears down all connections that are watching.
func (fb *Firebase) StopWatching() {
	fb.watchMtx.Lock()
	defer fb.watchMtx.Unlock()

	if fb.watching {
		// flip the bit back to not watching
		fb.watching = false
		// signal connection to terminate
		select {
		case fb.stopWatching <- struct{}{}:
		default:
		}
	}
}

func (fb *Firebase) setWatching(v bool) {
	fb.watchMtx.Lock()
	fb.watching = v
	fb.watchMtx.Unlock()
}

// Watch listens for changes on a firebase instance and
// passes over to the given chan.
//
// Only one connection can be established at a time. The
// second call to this function without a call to fb.StopWatching
// will close the channel given and return nil immediately.
func (fb *Firebase) Watch(notifications chan Event) error {
	return fb.WatchWithContext(context.Background(), notifications)
}

// WatchWithContext listens for changes on a firebase instance and
// passes over to the given chan. The provided context can be used
// for cancellation and deadline propagation.
//
// Only one connection can be established at a time. The
// second call to this function without a call to fb.StopWatching
// will close the channel given and return nil immediately.
func (fb *Firebase) WatchWithContext(ctx context.Context, notifications chan Event) error {
	fb.watchMtx.Lock()
	if fb.watching {
		fb.watchMtx.Unlock()
		close(notifications)
		return nil
	}
	// Drain any stale stop signal from a previous watch cycle
	// that ended on its own (e.g. heartbeat timeout) while a
	// concurrent StopWatching call raced in.
	select {
	case <-fb.stopWatching:
	default:
	}
	fb.watching = true
	fb.watchMtx.Unlock()

	stop := make(chan struct{})
	events, err := fb.watch(stop)
	if err != nil {
		return err
	}

	var closedManually atomic.Bool

	// done is closed when the event-forwarding goroutine exits,
	// allowing the stop-listener goroutine to clean up.
	done := make(chan struct{})

	var stopOnce sync.Once
	closeStop := func() {
		stopOnce.Do(func() { close(stop) })
	}

	go func() {
		select {
		case <-fb.stopWatching:
			closedManually.Store(true)
			closeStop()
		case <-ctx.Done():
			closedManually.Store(true)
			closeStop()
		case <-done:
			closeStop()
		}
	}()

	go func() {
		defer func() {
			close(notifications)
			fb.setWatching(false)
			close(done)
		}()

		for event := range events {
			if closedManually.Load() {
				return
			}

			notifications <- event
		}
	}()

	return nil
}

func readLine(rdr *bufio.Reader, prefix string) ([]byte, error) {
	// read event: line
	line, err := rdr.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	// empty line check for empty prefix
	if len(prefix) == 0 {
		line = bytes.TrimSpace(line)
		if len(line) != 0 {
			return nil, fmt.Errorf("expected empty line, got: %q", line)
		}
		return line, nil
	}

	// check line has event prefix
	if !bytes.HasPrefix(line, []byte(prefix)) {
		return nil, fmt.Errorf("missing prefix %q, got: %q", prefix, line)
	}

	// trim space
	line = line[len(prefix):]
	return bytes.TrimSpace(line), nil
}

func (fb *Firebase) watch(stop chan struct{}) (chan Event, error) {
	// build SSE request
	req, err := http.NewRequest("GET", fb.String(), nil)
	if err != nil {
		fb.setWatching(false)
		return nil, err
	}
	req.Header.Add("Accept", "text/event-stream")

	// do request
	resp, err := fb.client.Do(req)
	if err != nil {
		fb.setWatching(false)
		return nil, err
	}

	notifications := make(chan Event)

	// parserDone is closed when the parser goroutine exits,
	// ensuring the stop-listener and heartbeat goroutines
	// can always clean up regardless of how the watch ends.
	parserDone := make(chan struct{})

	var closeOnce sync.Once
	closeBody := func() {
		closeOnce.Do(func() {
			resp.Body.Close()
		})
	}

	go func() {
		select {
		case <-stop:
		case <-parserDone:
		}
		closeBody()
	}()

	heartbeat := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-parserDone:
				return
			case <-heartbeat:
				// do nothing
			case <-time.After(fb.watchHeartbeat):
				closeBody()
				return
			}
		}
	}()

	// start parsing response body
	go func() {
		defer func() {
			closeBody()
			close(notifications)
			close(parserDone)
		}()

		// build scanner for response body
		scanner := bufio.NewReader(resp.Body)
		sendError := func(err error) {
			notifications <- Event{
				Type: EventTypeError,
				Data: err,
			}
		}
		for {
			select {
			case heartbeat <- struct{}{}:
			default:
			}
			// scan for 'event:'
			evt, err := readLine(scanner, "event: ")
			if err != nil {
				sendError(err)
				return
			}

			// scan for 'data:'
			dat, err := readLine(scanner, "data: ")
			if err != nil {
				sendError(err)
				return
			}

			// read the empty line
			_, err = readLine(scanner, "")
			if err != nil {
				sendError(err)
				return
			}

			// create a base event
			event := Event{
				Type:    string(evt),
				Data:    string(dat),
				rawData: dat,
			}

			// should be reacting differently based off the type of event
			switch event.Type {
			case EventTypePut, EventTypePatch:
				// we've got extra data we've got to parse
				var data map[string]interface{}
				if err := json.Unmarshal(event.rawData, &data); err != nil {
					sendError(err)
					return
				}

				// set the extra fields
				event.Path = data["path"].(string)
				event.Data = data["data"]

				// ship it
				notifications <- event
			case eventTypeKeepAlive:
				// received ping - nothing to do here
			case eventTypeCancel:
				// The data for this event is null
				// This event will be sent if the Security and Firebase Rules
				// cause a read at the requested location to no longer be allowed

				// send the cancel event
				notifications <- event
				return
			case EventTypeAuthRevoked:
				// The data for this event is a string indicating that a the credential has expired
				// This event will be sent when the supplied auth parameter is no longer valid
				notifications <- event
				return
			case eventTypeRulesDebug:
				log.Printf("Rules-Debug: %s\n%s\n", evt, dat)
			}
		}
	}()
	return notifications, nil
}

const (
	initialBackoff = 500 * time.Millisecond
	maxBackoff     = 30 * time.Second
)

func backoffDuration(attempt int) time.Duration {
	if attempt > 30 {
		return maxBackoff
	}
	d := initialBackoff * time.Duration(1<<uint(attempt))
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}
