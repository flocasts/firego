# Firego

[![CI](https://github.com/flocasts/firego/actions/workflows/ci.yaml/badge.svg)](https://github.com/flocasts/firego/actions/workflows/ci.yaml)

A Firebase client written in Go

> **Note:** This is a FloSports fork of [zabawaba99/firego](https://github.com/zabawaba99/firego).

## Installation

```bash
go get github.com/flocasts/firego
```

## Usage

Import firego

```go
import "github.com/flocasts/firego"
```

Create a new firego reference

```go
f := firego.New("https://my-firebase-app.firebaseIO.com", nil)
```

with existing http client

```go
f := firego.New("https://my-firebase-app.firebaseIO.com", client)
```

### Request Timeouts

By default, the `Firebase` reference will timeout after 30 seconds of trying
to reach a Firebase server. You can configure this value by setting the global
timeout duration

```go
firego.TimeoutDuration = time.Minute
```

### Authentication

You can authenticate with your `service_account.json` file by using the
`golang.org/x/oauth2` package

```go
d, err := os.ReadFile("our_service_account.json")
if err != nil {
    return nil, err
}

conf, err := google.JWTConfigFromJSON(d, "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/firebase.database")
if err != nil {
    return nil, err
}

fb := firego.New("https://you.firebaseio.com", conf.Client(oauth2.NoContext))
// use the authenticated fb instance
```

### Legacy Tokens

```go
f.Auth("some-token-that-was-created-for-me")
f.Unauth()
```

### Get Value

```go
var v map[string]interface{}
if err := f.Value(&v); err != nil {
  log.Fatal(err)
}
fmt.Printf("%s\n", v)
```

#### Querying

Take a look at Firebase's [query parameters](https://www.firebase.com/docs/rest/guide/retrieving-data.html#section-rest-filtering)
for more information on what each function does.

```go
var v map[string]interface{}
if err := f.StartAt("a").EndAt("c").LimitToFirst(8).OrderBy("field").Value(&v); err != nil {
	log.Fatal(err)
}
fmt.Printf("%s\n", v)
```

### Set Value

```go
v := map[string]string{"foo":"bar"}
if err := f.Set(v); err != nil {
  log.Fatal(err)
}
```

### Push Value

```go
v := "bar"
pushedFirego, err := f.Push(v)
if err != nil {
	log.Fatal(err)
}

var bar string
if err := pushedFirego.Value(&bar); err != nil {
	log.Fatal(err)
}

// prints "https://my-firebase-app.firebaseIO.com/-JgvLHXszP4xS0AUN-nI: bar"
fmt.Printf("%s: %s\n", pushedFirego, bar)
```

### Update Child

```go
v := map[string]string{"foo":"bar"}
if err := f.Update(v); err != nil {
  log.Fatal(err)
}
```

### Remove Value

```go
if err := f.Remove(); err != nil {
  log.Fatal(err)
}
```

### Watch a Node

```go
notifications := make(chan firego.Event)
if err := f.Watch(notifications); err != nil {
	log.Fatal(err)
}

defer f.StopWatching()
for event := range notifications {
	fmt.Printf("Event %#v\n", event)
}
fmt.Printf("Notifications have stopped")
```

### Change reference

You can use a reference to save or read data from a specified reference

```go
userID := "bar"
usersRef,err := f.Ref("users/"+userID)
if err != nil {
  log.Fatal(err)
}
v := map[string]string{"id":userID}
if err := usersRef.Set(v); err != nil {
  log.Fatal(err)
}
```

Check the [Firebase Documentation](https://www.firebase.com/docs/rest/) for more details

## Running Tests

```bash
go test -race ./...
```

## Releases

This project uses semver tags (`v*.*.*`). Pushing a tag triggers a GitHub Actions workflow that creates a GitHub Release.
