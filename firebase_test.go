package firego

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"

	"github.com/flocasts/firego/firetest"
)

const URL = "https://somefirebaseapp.firebaseIO.com"

type TestServer struct {
	*httptest.Server
	receivedReqs []*http.Request
}

func newTestServer(response string) *TestServer {
	ts := &TestServer{}
	ts.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ts.receivedReqs = append(ts.receivedReqs, req)
		fmt.Fprint(w, response)
	}))
	return ts
}

func mustNew(t *testing.T, url string, opts ...option.ClientOption) *Firebase {
	t.Helper()
	fb, err := New(url, opts...)
	require.NoError(t, err)
	return fb
}

func TestNew(t *testing.T) {
	t.Parallel()
	testURLs := []string{
		URL,
		URL + "/",
		"somefirebaseapp.firebaseIO.com",
		"somefirebaseapp.firebaseIO.com/",
	}

	for _, url := range testURLs {
		fb := mustNew(t, url)
		assert.Equal(t, URL, fb.url, "givenURL: %s", url)
	}
}

func TestNewWithProvidedHttpClient(t *testing.T) {
	t.Parallel()

	client := http.DefaultClient
	testURLs := []string{
		URL,
		URL + "/",
		"somefirebaseapp.firebaseIO.com",
		"somefirebaseapp.firebaseIO.com/",
	}

	for _, url := range testURLs {
		fb := mustNew(t, url, option.WithHTTPClient(client))
		assert.Equal(t, URL, fb.url, "givenURL: %s", url)
	}
}

func TestAuth(t *testing.T) {
	t.Parallel()
	server := firetest.New()
	server.Start()
	defer server.Close()

	server.RequireAuth(true)
	fb := mustNew(t, server.URL, option.WithTokenSource(
		oauth2.StaticTokenSource(&oauth2.Token{AccessToken: server.Secret}),
	))

	var v interface{}
	err := fb.Value(&v)
	assert.NoError(t, err)
}

func TestUnauthenticated(t *testing.T) {
	t.Parallel()
	server := firetest.New()
	server.Start()
	defer server.Close()

	server.RequireAuth(true)
	fb := mustNew(t, server.URL)

	err := fb.Value("")
	assert.Error(t, err)
}

func TestPush(t *testing.T) {
	t.Parallel()
	var (
		payload = map[string]interface{}{"foo": "bar"}
		server  = firetest.New()
	)
	server.Start()
	defer server.Close()

	fb := mustNew(t, server.URL)
	childRef, err := fb.Push(payload)
	assert.NoError(t, err)

	path := strings.TrimPrefix(childRef.String(), server.URL+"/")
	v := server.Get(path)
	assert.Equal(t, payload, v)
}

func TestRemove(t *testing.T) {
	t.Parallel()
	server := firetest.New()
	server.Start()
	defer server.Close()

	server.Set("", true)

	fb := mustNew(t, server.URL)
	err := fb.Remove()
	assert.NoError(t, err)

	v := server.Get("")
	assert.Nil(t, v)
}

func TestSet(t *testing.T) {
	t.Parallel()
	var (
		payload = map[string]interface{}{"foo": "bar"}
		server  = firetest.New()
	)
	server.Start()
	defer server.Close()

	fb := mustNew(t, server.URL)
	err := fb.Set(payload)
	assert.NoError(t, err)

	v := server.Get("")
	assert.Equal(t, payload, v)
}

func TestUpdate(t *testing.T) {
	t.Parallel()
	var (
		payload = map[string]interface{}{"foo": "bar"}
		server  = firetest.New()
	)
	server.Start()
	defer server.Close()

	fb := mustNew(t, server.URL)
	err := fb.Update(payload)
	assert.NoError(t, err)

	v := server.Get("")
	assert.Equal(t, payload, v)
}

func TestValue(t *testing.T) {
	t.Parallel()
	var (
		response = map[string]interface{}{"foo": "bar"}
		server   = firetest.New()
	)
	server.Start()
	defer server.Close()

	fb := mustNew(t, server.URL)

	server.Set("", response)

	var v map[string]interface{}
	err := fb.Value(&v)
	assert.NoError(t, err)
	assert.Equal(t, response, v)
}

func TestChild(t *testing.T) {
	t.Parallel()
	var (
		parent    = mustNew(t, URL)
		childNode = "node"
		child     = parent.Child(childNode)
	)

	assert.Equal(t, fmt.Sprintf("%s/%s", parent.url, childNode), child.url)
}

func TestChild_Issue26(t *testing.T) {
	t.Parallel()
	parent := mustNew(t, URL)
	child1 := parent.Child("one")
	child2 := child1.Child("two")

	child1.Shallow(true)
	assert.Len(t, child2.params, 0)
}

func TestTimeoutDuration_Headers(t *testing.T) {
	var fb *Firebase
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		time.Sleep(2 * fb.clientTimeout)
		close(done)
	}))
	defer server.Close()

	fb = mustNew(t, server.URL)
	fb.clientTimeout = time.Millisecond
	err := fb.Value("")
	<-done
	assert.NotNil(t, err)
	assert.IsType(t, ErrTimeout{}, err)

	// ResponseHeaderTimeout should be TimeoutDuration less the time it took to dial, and should be positive
	require.IsType(t, (*http.Transport)(nil), fb.client.Transport)
	tr := fb.client.Transport.(*http.Transport)
	assert.True(t, tr.ResponseHeaderTimeout < TimeoutDuration)
	assert.True(t, tr.ResponseHeaderTimeout > 0)
}

func TestTimeoutDuration_Dial(t *testing.T) {
	fb := mustNew(t, "http://dialtimeouterr.or/")
	fb.clientTimeout = time.Millisecond

	err := fb.Value("")
	assert.NotNil(t, err)
	assert.IsType(t, ErrTimeout{}, err)

	// ResponseHeaderTimeout should be negative since the total duration was consumed when dialing
	require.IsType(t, (*http.Transport)(nil), fb.client.Transport)
	assert.True(t, fb.client.Transport.(*http.Transport).ResponseHeaderTimeout < 0)
}
