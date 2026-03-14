/*
Package firego is a REST client for Firebase (https://firebase.com).
*/
package firego

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	_url "net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
)

// Firebase OAuth2 scopes required for Realtime Database access.
const (
	ScopeFirebaseDatabase = "https://www.googleapis.com/auth/firebase.database"
	ScopeUserinfoEmail    = "https://www.googleapis.com/auth/userinfo.email"
)

// TimeoutDuration is the length of time any request will have to establish
// a connection and receive headers from Firebase before returning
// an ErrTimeout error.
var TimeoutDuration = 30 * time.Second

var defaultRedirectLimit = 30

// ErrTimeout is an error type is that is returned if a request
// exceeds the TimeoutDuration configured.
type ErrTimeout struct {
	error
}

// FirebaseError is returned when Firebase responds with a non-2xx status code.
// It exposes both the HTTP status code and the response body so callers can
// programmatically inspect the failure.
type FirebaseError struct {
	StatusCode int
	Body       string
}

func (e *FirebaseError) Error() string {
	return fmt.Sprintf("firebase: http error %d: %s", e.StatusCode, e.Body)
}

// query parameter constants
const (
	shallowParam      = "shallow"
	formatParam       = "format"
	formatVal         = "export"
	orderByParam      = "orderBy"
	limitToFirstParam = "limitToFirst"
	limitToLastParam  = "limitToLast"
	startAtParam      = "startAt"
	endAtParam        = "endAt"
	equalToParam      = "equalTo"
)

const defaultHeartbeat = 2 * time.Minute

// Firebase represents a location in the cloud.
type Firebase struct {
	url           string
	client        *http.Client
	clientTimeout time.Duration

	paramsMtx sync.RWMutex
	params    _url.Values

	eventMtx   sync.Mutex
	eventFuncs map[string]chan struct{}

	watchMtx       sync.Mutex
	watching       bool
	watchHeartbeat time.Duration
	stopWatching   chan struct{}
}

// New creates a new Firebase reference.
//
// When called with no options, a default HTTP client is used.
// To authenticate with Firebase, pass option.ClientOption values
// such as option.WithCredentialsFile or option.WithTokenSource:
//
//	fb, err := firego.New("https://my-app.firebaseio.com",
//	    option.WithCredentialsFile("service_account.json"),
//	)
func New(url string, opts ...option.ClientOption) (*Firebase, error) {
	fb := &Firebase{
		url:            sanitizeURL(url),
		params:         _url.Values{},
		clientTimeout:  TimeoutDuration,
		stopWatching:   make(chan struct{}, 1),
		watchHeartbeat: defaultHeartbeat,
		eventFuncs:     map[string]chan struct{}{},
	}

	if len(opts) == 0 {
		var tr *http.Transport
		tr = &http.Transport{
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				start := time.Now()
				dialer := net.Dialer{Timeout: fb.clientTimeout}
				c, err := dialer.DialContext(ctx, network, address)
				tr.ResponseHeaderTimeout = fb.clientTimeout - time.Since(start)
				return c, err
			},
		}
		fb.client = &http.Client{
			Transport:     tr,
			CheckRedirect: redirectPreserveHeaders,
		}
		return fb, nil
	}

	// Prepend Firebase scopes so callers don't have to specify them.
	allOpts := []option.ClientOption{
		option.WithScopes(ScopeFirebaseDatabase, ScopeUserinfoEmail),
	}
	allOpts = append(allOpts, opts...)

	client, _, err := htransport.NewClient(context.Background(), allOpts...)
	if err != nil {
		return nil, fmt.Errorf("firego: failed to create authenticated client: %w", err)
	}
	client.CheckRedirect = redirectPreserveHeaders
	fb.client = client
	return fb, nil
}

// NewWithTokenSource creates a new Firebase reference authenticated with the
// given oauth2.TokenSource. This is a convenience wrapper around New.
func NewWithTokenSource(url string, ts oauth2.TokenSource) (*Firebase, error) {
	return New(url, option.WithTokenSource(ts))
}

// Ref returns a copy of an existing Firebase reference with a new path.
func (fb *Firebase) Ref(path string) (*Firebase, error) {
	newFB := fb.copy()
	parsedURL, err := _url.Parse(fb.url)
	if err != nil {
		return newFB, err
	}
	newFB.url = parsedURL.Scheme + "://" + parsedURL.Host + "/" + strings.Trim(path, "/")
	return newFB, nil
}

// SetURL changes the url for a firebase reference.
func (fb *Firebase) SetURL(url string) {
	fb.url = sanitizeURL(url)
}

// URL returns firebase reference URL
func (fb *Firebase) URL() string {
	return fb.url
}

// Push creates a reference to an auto-generated child location.
func (fb *Firebase) Push(v interface{}) (*Firebase, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	_, data, err = fb.doRequest("POST", data)
	if err != nil {
		return nil, err
	}
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	newRef := fb.copy()
	newRef.url = fb.url + "/" + m["name"]
	return newRef, err
}

// Remove the Firebase reference from the cloud.
func (fb *Firebase) Remove() error {
	_, _, err := fb.doRequest("DELETE", nil)
	if err != nil {
		return err
	}
	return nil
}

// Set the value of the Firebase reference.
func (fb *Firebase) Set(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, _, err = fb.doRequest("PUT", data)
	return err
}

// Update the specific child with the given value.
func (fb *Firebase) Update(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, _, err = fb.doRequest("PATCH", data)
	return err
}

// Value gets the value of the Firebase reference.
func (fb *Firebase) Value(v interface{}) error {
	_, respBody, err := fb.doRequest("GET", nil)
	if err != nil {
		return err
	}
	return json.Unmarshal(respBody, v)
}

// String returns the string representation of the
// Firebase reference.
func (fb *Firebase) String() string {
	path := fb.url + "/.json"

	fb.paramsMtx.RLock()
	if len(fb.params) > 0 {
		path += "?" + fb.params.Encode()
	}
	fb.paramsMtx.RUnlock()
	return path
}

// Child creates a new Firebase reference for the requested
// child with the same configuration as the parent.
func (fb *Firebase) Child(child string) *Firebase {
	c := fb.copy()
	c.url = c.url + "/" + child
	return c
}

func (fb *Firebase) copy() *Firebase {
	c := &Firebase{
		url:            fb.url,
		params:         _url.Values{},
		client:         fb.client,
		clientTimeout:  fb.clientTimeout,
		stopWatching:   make(chan struct{}, 1),
		watchHeartbeat: defaultHeartbeat,
		eventFuncs:     map[string]chan struct{}{},
	}

	// making sure to manually copy the map items into a new
	// map to avoid modifying the map reference.
	fb.paramsMtx.RLock()
	for k, v := range fb.params {
		c.params[k] = v
	}
	fb.paramsMtx.RUnlock()
	return c
}

func sanitizeURL(url string) string {
	if !strings.HasPrefix(url, "https://") && !strings.HasPrefix(url, "http://") {
		url = "https://" + url
	}

	if strings.HasSuffix(url, "/") {
		url = url[:len(url)-1]
	}

	return url
}

// Preserve headers on redirect.
//
// Reference https://github.com/golang/go/issues/4800
func redirectPreserveHeaders(req *http.Request, via []*http.Request) error {
	if len(via) == 0 {
		// No redirects
		return nil
	}

	if len(via) > defaultRedirectLimit {
		return fmt.Errorf("%d consecutive requests(redirects)", len(via))
	}

	// mutate the subsequent redirect requests with the first Header
	for key, val := range via[0].Header {
		req.Header[key] = val
	}
	return nil
}

func withHeader(key, value string) func(*http.Request) {
	return func(req *http.Request) {
		req.Header.Add(key, value)
	}
}

func (fb *Firebase) doRequest(method string, body []byte, options ...func(*http.Request)) (http.Header, []byte, error) {
	return fb.doRequestWithContext(context.Background(), method, body, options...)
}

func (fb *Firebase) doRequestWithContext(ctx context.Context, method string, body []byte, options ...func(*http.Request)) (http.Header, []byte, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	} else {
		bodyReader = http.NoBody
	}

	req, err := http.NewRequestWithContext(ctx, method, fb.String(), bodyReader)
	if err != nil {
		return nil, nil, err
	}

	for _, opt := range options {
		opt(req)
	}

	resp, err := fb.client.Do(req)
	switch err := err.(type) {
	default:
		return nil, nil, err
	case nil:
		// carry on

	case *_url.Error:
		// `http.Client.Do` will return a `url.Error` that wraps a `net.Error`
		// when exceeding it's `Transport`'s `ResponseHeadersTimeout`
		e1, ok := err.Err.(net.Error)
		if ok && e1.Timeout() {
			return nil, nil, ErrTimeout{err}
		}

		return nil, nil, err

	case net.Error:
		// `http.Client.Do` will return a `net.Error` directly when Dial times
		// out, or when the Client's RoundTripper otherwise returns an err
		if err.Timeout() {
			return nil, nil, ErrTimeout{err}
		}

		return nil, nil, err
	}

	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.Header, respBody, &FirebaseError{
			StatusCode: resp.StatusCode,
			Body:       string(respBody),
		}
	}
	return resp.Header, respBody, nil
}
