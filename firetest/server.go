/*
Package firetest provides utilities for Firebase testing
*/
package firetest

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

var (
	missingJSONExtension = []byte("append .json to your request URI to use the REST API")
	missingBody          = []byte(`{"error":"Error: No data supplied."}`)
	invalidJSON          = []byte(`{"error":"Invalid data; couldn't parse JSON object, array, or value. Perhaps you're using invalid characters in your key names."}`)
	invalidAuth          = []byte(`{"error" : "Could not parse auth token."}`)
)

// Firetest is a Firebase server implementation
type Firetest struct {
	// URL of form http://ipaddr:port with no trailing slash
	URL string
	// Secret used to authenticate with server
	Secret string

	listener net.Listener
	db       *notifyDB

	requireAuth *int32
}

// New creates a new Firetest server
func New() *Firetest {
	secret := []byte(fmt.Sprint(time.Now().UnixNano()))
	return &Firetest{
		db:          newNotifyDB(),
		Secret:      base64.URLEncoding.EncodeToString(secret),
		requireAuth: new(int32),
	}
}

// Start starts the server
func (ft *Firetest) Start() {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if l, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			panic(fmt.Errorf("failed to listen on a port: %v", err))
		}
	}
	ft.listener = l

	s := http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ft.serveHTTP(w, req)
	})}
	go func() {
		if err := s.Serve(l); err != nil {
			log.Printf("error serving: %s", err)
		}

		ft.Close()
	}()
	ft.URL = "http://" + ft.listener.Addr().String()
}

// Close closes the server
func (ft *Firetest) Close() {
	if ft.listener != nil {
		ft.listener.Close()
	}
}

func (ft *Firetest) serveHTTP(w http.ResponseWriter, req *http.Request) {
	if !strings.HasSuffix(req.URL.Path, ".json") {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(missingJSONExtension))
		return
	}

	if atomic.LoadInt32(ft.requireAuth) == 1 {
		authHeader := req.Header.Get("Authorization")
		token := strings.TrimPrefix(authHeader, "Bearer ")
		if authHeader == token || token != ft.Secret {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write(invalidAuth)
			return
		}
	}

	switch req.Method {
	case "PUT":
		ft.set(w, req)
	case "PATCH":
		ft.update(w, req)
	case "POST":
		ft.create(w, req)
	case "GET":
		switch req.Header.Get("Accept") {
		case "text/event-stream":
			ft.sse(w, req)
		default:
			ft.get(w, req)
		}
	case "DELETE":
		ft.del(w, req)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.Println("not implemented yet")
	}
}

func (ft *Firetest) set(w http.ResponseWriter, req *http.Request) {
	body, v, ok := unmarshal(w, req.Body)
	if !ok {
		return
	}

	ft.Set(req.URL.Path, v)
	w.Write(body)
}

func (ft *Firetest) update(w http.ResponseWriter, req *http.Request) {
	body, v, ok := unmarshal(w, req.Body)
	if !ok {
		return
	}
	ft.Update(req.URL.Path, v)
	w.Write(body)
}

func (ft *Firetest) create(w http.ResponseWriter, req *http.Request) {
	_, v, ok := unmarshal(w, req.Body)
	if !ok {
		return
	}

	name := ft.Create(req.URL.Path, v)
	rtn := map[string]string{"name": name}
	if err := json.NewEncoder(w).Encode(rtn); err != nil {
		log.Printf("Error encoding json: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (ft *Firetest) del(w http.ResponseWriter, req *http.Request) {
	ft.Delete(req.URL.Path)
}

func (ft *Firetest) get(w http.ResponseWriter, req *http.Request) {
	w.Header().Add("Content-Type", "application/json")

	v := ft.Get(req.URL.Path)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("Error encoding json: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (ft *Firetest) sse(w http.ResponseWriter, req *http.Request) {
	f, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming is not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")

	path := sanitizePath(req.URL.Path)
	c := ft.db.watch(path)
	defer ft.db.stopWatching(path, c)

	d := eventData{Path: path, Data: ft.db.get(path)}
	s, err := json.Marshal(d)
	if err != nil {
		fmt.Printf("Error marshaling node %s\n", err)
	}
	fmt.Fprintf(w, "event: put\ndata: %s\n\n", s)
	f.Flush()

	ctx := req.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
			fmt.Fprintf(w, "event: keep-alive\ndata: null\n\n")
			f.Flush()
			continue
		case n, ok := <-c:
			if !ok {
				return
			}

			s, err := json.Marshal(n.Data)
			if err != nil {
				fmt.Printf("Error marshaling node %s\n", err)
				continue
			}

			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", n.Name, s)
			f.Flush()
		}
	}
}

func sanitizePath(p string) string {
	// remove slashes from the front and back
	//	/foo/.json -> foo/.json
	s := strings.Trim(p, "/")

	// remove .json extension
	//	foo/.json -> foo/
	s = strings.TrimSuffix(s, ".json")

	// trim an potential trailing slashes
	//	foo/ -> foo
	return strings.TrimSuffix(s, "/")
}

func unmarshal(w http.ResponseWriter, r io.Reader) ([]byte, interface{}, bool) {
	body, err := io.ReadAll(r)
	if err != nil || len(body) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		w.Write(missingBody)
		return nil, nil, false
	}

	var v interface{}
	if err := json.Unmarshal(body, &v); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write(invalidJSON)
		return nil, nil, false
	}
	return body, v, true
}
