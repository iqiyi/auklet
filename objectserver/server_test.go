// Copyright (c) 2015 Rackspace
// Copyright (c) 2016-2018 iQIYI.com.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectserver

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/conf"
	"github.com/iqiyi/auklet/common/ring"

	"github.com/stretchr/testify/require"
)

var (
	GetRing = ring.GetRing
)

type TestServer struct {
	*httptest.Server
	host      string
	port      int
	root      string
	objServer *ObjectServer
}

func (t *TestServer) Close() {
	t.objServer.Finalize()
	os.RemoveAll(t.root)
	t.Server.Close()
}

func (t *TestServer) Do(method string, path string, body io.ReadCloser) (*http.Response, error) {
	req, err := http.NewRequest(method, fmt.Sprintf("http://%s:%d%s", t.host, t.port, path), body)
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

var testObjectServers uint64 = 0

func makeObjectServer(settings ...string) (*TestServer, error) {
	driveRoot, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, err
	}

	// bind_port here is actually a pseudo config intended to distinguish
	// the metrics collector
	confTemplate := `
[app:object-server]
devices=%s
mount_check=false
bind_port=%d
rpc_port=%d
async_job_manager=fs
test_mode = yes
	`
	configString := fmt.Sprintf(confTemplate, driveRoot,
		common.RandIntInRange(40000, 50000), common.RandIntInRange(50001, 60000))

	for i := 0; i < len(settings); i += 2 {
		configString += fmt.Sprintf("%s=%s\n", settings[i], settings[i+1])
	}
	conf, err := conf.StringConfig(configString)
	if err != nil {
		return nil, err
	}

	flags := flag.NewFlagSet("object server ut", flag.ExitOnError)
	flags.String("l", "", "zap yaml log config file")
	s, err := InitServer(conf, flags)
	if err != nil {
		return nil, err
	}
	server := s.(*ObjectServer)
	ts := httptest.NewServer(server.buildHandler(conf))
	u, err := url.Parse(ts.URL)
	if err != nil {
		return nil, err
	}
	host, ports, err := net.SplitHostPort(u.Host)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(ports)
	if err != nil {
		return nil, err
	}

	server.hashPrefix = ""
	server.hashSuffix = "changeme"

	mgr := server.asyncJobMgr.(*FSAsyncJobMgr)
	mgr.hashPrefix = server.hashPrefix
	mgr.hashSuffix = server.hashSuffix

	return &TestServer{
		Server:    ts,
		host:      host,
		port:      port,
		root:      driveRoot,
		objServer: server,
	}, nil
}

func TestBasicPutGet(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	defer ts.Close()

	timestamp := common.GetTimestamp()
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), bytes.NewBuffer([]byte("SOME DATA")))
	require.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "9")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 201, resp.StatusCode)

	resp, err = ts.Do("GET", "/sda/0/a/c/o", nil)
	require.Nil(t, err)
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, timestamp, resp.Header.Get("X-Timestamp"))
	require.Equal(t, "9", resp.Header.Get("Content-Length"))
}

func TestBasicPutDelete(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	defer ts.Close()

	timestamp := common.GetTimestamp()
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), bytes.NewBuffer([]byte("SOME DATA")))
	require.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "9")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 201, resp.StatusCode)

	timestamp = common.GetTimestamp()
	req, err = http.NewRequest("DELETE", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", timestamp)
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 204, resp.StatusCode)

	resp, err = ts.Do("GET", "/sda/0/a/c/o", nil)
	require.Nil(t, err)
	require.Equal(t, 404, resp.StatusCode)
}

func TestBasicPutPostGet(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	defer ts.Close()

	timestamp := common.GetTimestamp()
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), bytes.NewBuffer([]byte("SOME DATA")))
	require.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "9")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 201, resp.StatusCode)

	timestamp = common.GetTimestamp()
	req, err = http.NewRequest("POST", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
	require.Nil(t, err)
	req.Header.Set("X-Object-Meta-TestPutPostGet", "Hi!")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 202, resp.StatusCode)

	resp, err = ts.Do("GET", "/sda/0/a/c/o", nil)
	require.Nil(t, err)
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, timestamp, resp.Header.Get("X-Timestamp"))
	require.Equal(t, "9", resp.Header.Get("Content-Length"))
	require.Equal(t, "Hi!", resp.Header.Get("X-Object-Meta-TestPutPostGet"))
}

func TestPostContentType(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	defer ts.Close()

	timestamp := common.GetTimestamp()
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), bytes.NewBuffer([]byte("SOME DATA")))
	require.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "9")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 201, resp.StatusCode)

	timestamp = common.GetTimestamp()
	req, err = http.NewRequest("POST", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
	require.Nil(t, err)
	req.Header.Set("Content-Type", "any/thing")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 409, resp.StatusCode)
}

func TestPostNotFound(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	defer ts.Close()

	timestamp := common.GetTimestamp()
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
	require.Nil(t, err)
	req.Header.Set("X-Object-Meta-TestPostNotFound", "Howdy!")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 404, resp.StatusCode)
}

func TestGetRanges(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	require.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "26")
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 201, resp.StatusCode)

	getRanges := func(ranges string) (*http.Response, []byte) {
		req, err := http.NewRequest("GET", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
		require.Nil(t, err)
		req.Header.Set("Range", ranges)
		resp, err := http.DefaultClient.Do(req)
		require.Nil(t, err)
		body, err := ioutil.ReadAll(resp.Body)
		require.Nil(t, err)
		return resp, body
	}

	resp, body := getRanges("bytes=0-5")
	require.Equal(t, http.StatusPartialContent, resp.StatusCode)
	require.Equal(t, "ABCDEF", string(body))

	resp, body = getRanges("bytes=20-")
	require.Equal(t, http.StatusPartialContent, resp.StatusCode)
	require.Equal(t, "UVWXYZ", string(body))

	resp, body = getRanges("bytes=-6")
	require.Equal(t, http.StatusPartialContent, resp.StatusCode)
	require.Equal(t, "UVWXYZ", string(body))

	resp, body = getRanges("bytes=27-28")
	require.Equal(t, http.StatusRequestedRangeNotSatisfiable, resp.StatusCode)

	resp, body = getRanges("bytes=20-,-6")
	require.Equal(t, http.StatusPartialContent, resp.StatusCode)
	require.True(t, strings.HasPrefix(resp.Header.Get("Content-Type"), "multipart/byteranges;boundary="))
	require.Equal(t, "366", resp.Header.Get("Content-Length"))
	require.Equal(t, 2, strings.Count(string(body), "UVWXYZ"))
}

func TestBadEtag(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	require.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "26")
	req.Header.Set("ETag", "11111111111111111111111111111111")
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 422, resp.StatusCode)
}

func TestCorrectEtag(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	require.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "26")
	etag := "437bba8e0bf58337674f4539e75186ac"
	req.Header.Set("ETag", etag)
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 201, resp.StatusCode)
	require.Equal(t, etag, resp.Header.Get("Etag"))
}

func TestUppercaseEtag(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	require.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "26")
	req.Header.Set("ETag", "437BBA8E0BF58337674F4539E75186AC")
	req.Header.Set("X-Timestamp", common.GetTimestamp())
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 201, resp.StatusCode)
	require.Equal(t, "437bba8e0bf58337674f4539e75186ac", resp.Header.Get("Etag"))
}

type shortReader struct{}

func (s *shortReader) Read(p []byte) (n int, err error) {
	return 0, io.ErrUnexpectedEOF
}

type fakeResponse struct {
	status int
}

func (*fakeResponse) Header() http.Header {
	return make(http.Header)
}

func (*fakeResponse) Write(p []byte) (int, error) {
	return len(p), nil
}

func (f *fakeResponse) WriteHeader(s int) {
	f.status = s
}

func TestDisconnectOnPut(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	defer ts.Close()

	timestamp := common.GetTimestamp()
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), &shortReader{})
	require.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "10")
	req.Header.Set("X-Timestamp", timestamp)

	resp := &fakeResponse{}

	ts.Server.Config.Handler.ServeHTTP(resp, req)
	require.Equal(t, resp.status, 499)
}

func TestEmptyDevice(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d//0/a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	require.Nil(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 400, resp.StatusCode)
}

func TestEmptyPartition(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda//a/c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	require.Nil(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 400, resp.StatusCode)
}

func TestEmptyAccount(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0//c/o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	require.Nil(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 400, resp.StatusCode)
}

func TestEmptyContainer(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a//o", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	require.Nil(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 400, resp.StatusCode)
}

func TestEmptyObject(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	defer ts.Close()

	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/", ts.host, ts.port),
		bytes.NewBuffer([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	require.Nil(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 400, resp.StatusCode)
}

func TestBasicPutDeleteAt(t *testing.T) {
	ts, err := makeObjectServer()
	require.Nil(t, err)
	defer ts.Close()

	timestamp := common.GetTimestamp()
	time_unix := int(time.Now().Unix())
	time_unix += 30
	time_delete := strconv.Itoa(time_unix)

	//put file with x-delete header
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), bytes.NewBuffer([]byte("SOME DATA")))
	require.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "9")
	req.Header.Set("X-Timestamp", timestamp)

	req.Header.Set("X-Delete-At", time_delete)
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 201, resp.StatusCode)

	timestamp = common.GetTimestamp()
	req, err = http.NewRequest("DELETE", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", timestamp)
	req.Header.Set("X-If-Delete-At", time_delete)
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 204, resp.StatusCode)

	resp, err = ts.Do("GET", "/sda/0/a/c/o", nil)
	require.Nil(t, err)
	require.Equal(t, 404, resp.StatusCode)

	//put file without x-delete header
	timestamp = common.GetTimestamp()
	req, err = http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), bytes.NewBuffer([]byte("SOME DATA")))
	require.Nil(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "9")
	req.Header.Set("X-Timestamp", timestamp)
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 201, resp.StatusCode)

	timestamp = common.GetTimestamp()
	req, err = http.NewRequest("DELETE", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", timestamp)
	req.Header.Set("X-If-Delete-At", time_delete)
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 412, resp.StatusCode)

	resp, err = ts.Do("GET", "/sda/0/a/c/o", nil)
	require.Nil(t, err)
	require.Equal(t, 200, resp.StatusCode)
}

type slowReader struct {
	readChan chan int
	id       int
}

func (s slowReader) Read(p []byte) (i int, err error) {
	data := <-s.readChan
	if data == 1 {
		time.Sleep(time.Second * 1)
	}
	if data == 2 {
		return 0, io.EOF
	}

	p[0] = byte('a')
	return 1, nil
}

func (s *slowReader) Close() error {
	return nil
}

func TestAcquireDevice(t *testing.T) {
	ts, err := makeObjectServer("disk_limit", "1/10")
	require.Nil(t, err)
	defer ts.Close()

	sr1 := slowReader{id: 1, readChan: make(chan int)}
	req1, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o", ts.host, ts.port), sr1)
	require.Nil(t, err)
	req1.Header.Set("Content-Type", "text")
	req1.Header.Set("Content-Length", "1")
	req1.Header.Set("X-Timestamp", common.GetTimestamp())

	sr2 := slowReader{id: 2, readChan: make(chan int)}
	req2, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/a/c/o2", ts.host, ts.port), sr2)
	require.Nil(t, err)
	req2.Header.Set("Content-Type", "text")
	req2.Header.Set("Content-Length", "1")
	req2.Header.Set("X-Timestamp", common.GetTimestamp())

	done1 := make(chan bool)

	go func() {
		resp, err := http.DefaultClient.Do(req1)
		require.Equal(t, resp.StatusCode, 201)
		require.Nil(t, err)
		done1 <- true

	}()
	done2 := make(chan bool)

	go func() {
		resp, err := http.DefaultClient.Do(req2)
		require.Nil(t, err)
		require.Equal(t, resp.StatusCode, 503)
		require.Equal(t, resp.Header.Get("X-Disk-Usage"), "1")
		done2 <- true
	}()
	//sending good read to 1
	sr1.readChan <- 0
	//sending sleep to 2, let #1 win the race
	sr2.readChan <- 1
	//sending good read to 2
	sr2.readChan <- 0
	//sending EOF to 2
	sr2.readChan <- 2
	//2 exiting goroutine
	<-done2
	//sending EOF to 1
	sr1.readChan <- 2
	//1 exiting goroutine
	<-done1
}

func TestAccountAcquireDevice(t *testing.T) {
	ts, err := makeObjectServer("account_rate_limit", "1/0")
	require.Nil(t, err)
	defer ts.Close()

	sr1 := slowReader{id: 1, readChan: make(chan int)}
	req1, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/α/c/o", ts.host, ts.port), sr1)
	require.Nil(t, err)
	req1.Header.Set("Content-Type", "text")
	req1.Header.Set("Content-Length", "1")
	req1.Header.Set("X-Timestamp", common.GetTimestamp())

	sr2 := slowReader{id: 2, readChan: make(chan int)}
	req2, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/sda/0/α/c/o2", ts.host, ts.port), sr2)
	require.Nil(t, err)
	req2.Header.Set("Content-Type", "text")
	req2.Header.Set("Content-Length", "1")
	req2.Header.Set("X-Timestamp", common.GetTimestamp())

	done1 := make(chan bool)

	go func() {
		resp, err := http.DefaultClient.Do(req1)
		require.Equal(t, resp.StatusCode, 201)
		require.Nil(t, err)
		done1 <- true

	}()
	done2 := make(chan bool)

	go func() {
		resp, err := http.DefaultClient.Do(req2)
		require.Nil(t, err)
		require.Equal(t, resp.StatusCode, 498)
		require.Equal(t, resp.Header.Get("X-Disk-Usage"), "")
		done2 <- true

	}()
	//sending good read to 1
	sr1.readChan <- 0
	//sending sleep to 2, let #1 win the race
	sr2.readChan <- 1
	//sending good read to 2
	sr2.readChan <- 0
	//sending EOF to 2
	sr2.readChan <- 2
	//2 exiting goroutine
	<-done2
	//sending EOF to 1
	sr1.readChan <- 2
	//1 exiting goroutine
	<-done1
}
