// Copyright (c) 2016 Rackspace
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

package test

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/iqiyi/auklet/common/ring"
)

// a place for utility functions and interface satisifiers that are used across tests

type CaptureResponse struct {
	Status int
	header http.Header
	Body   *bytes.Buffer
}

func (w *CaptureResponse) WriteHeader(status int) {
	w.Status = status
}

func (w *CaptureResponse) Header() http.Header {
	return w.header
}

func (w *CaptureResponse) Write(b []byte) (int, error) {
	return w.Body.Write(b)
}

func MakeCaptureResponse() *CaptureResponse {
	return &CaptureResponse{
		Status: 0,
		header: make(http.Header),
		Body:   new(bytes.Buffer),
	}
}

// FakeRing
type FakeRing struct {
	// Overrides for function returns
	MockLocalDevices       []*ring.Device
	MockGetJobNodes        []*ring.Device
	MockGetJobNodesHandoff bool
	MockGetMoreNodes       ring.MoreNodes

	// Used for probe
	MockMoreNodes *ring.Device
	MockDevices   []*ring.Device
}

func (r *FakeRing) GetNodes(partition uint64) (response []*ring.Device) {
	if len(r.MockDevices) > 0 {
		return r.MockDevices[0:3]
	} else {
		return nil
	}
}

func (r *FakeRing) GetJobNodes(partition uint64, localDevice int) (response []*ring.Device, handoff bool) {
	if len(r.MockDevices) > 0 {
		switch localDevice {
		case 0:
			return []*ring.Device{r.MockDevices[1], r.MockDevices[2]}, false
		case 1:
			return []*ring.Device{r.MockDevices[0], r.MockDevices[2]}, false
		case 2:
			return []*ring.Device{r.MockDevices[0], r.MockDevices[1]}, false
		default:
			return r.MockDevices[0:3], true
		}
	} else if len(r.MockGetJobNodes) > 0 {
		return r.MockGetJobNodes, r.MockGetJobNodesHandoff
	} else {
		return []*ring.Device{
			{Device: "sda", ReplicationIp: "127.0.0.1", ReplicationPort: 20000},
			{Device: "sdb", ReplicationIp: "127.0.0.2", ReplicationPort: 2000},
		}, r.MockGetJobNodesHandoff
	}
}

func (r *FakeRing) GetPartition(account string, container string, object string) uint64 {
	return 0
}

func (r *FakeRing) LocalDevices(localPort int) (devs []*ring.Device, err error) {
	if len(r.MockDevices) > 0 {
		for _, d := range r.MockDevices {
			if d.ReplicationPort == localPort {
				return []*ring.Device{d}, nil
			}
		}
		return nil, nil
	} else if len(r.MockLocalDevices) > 0 {
		return r.MockLocalDevices, nil
	} else {
		return nil, nil
	}
}

func (r *FakeRing) AllDevices() (devs []ring.Device) {
	return nil
}

func (r *FakeRing) GetMoreNodes(partition uint64) ring.MoreNodes {
	if r.MockMoreNodes != nil {
		return &fakeMoreNodes{r.MockMoreNodes}
	} else if r.MockGetMoreNodes != nil {
		return r.MockGetMoreNodes
	}
	return nil
}

func (r *FakeRing) PartitionCount() uint64 {
	return 1
}

func (r *FakeRing) ReplicaCount() uint64 {
	return 3
}

type fakeMoreNodes struct {
	dev *ring.Device
}

func (m *fakeMoreNodes) Next() *ring.Device {
	return m.dev
}

// Fake MemcacheRing
type FakeMemcacheRing struct {
	MockIncrResults   []int64
	MockIncrKeys      []string
	MockSetValues     []interface{}
	MockGetStructured map[string][]byte
}

func (mr *FakeMemcacheRing) Decr(key string, delta int64, timeout int) (int64, error) {
	return int64(0), nil
}

func (mr *FakeMemcacheRing) Delete(key string) error {
	return nil
}

func (mr *FakeMemcacheRing) Get(key string) (interface{}, error) {
	return nil, nil
}

func (mr *FakeMemcacheRing) GetStructured(key string, val interface{}) error {
	if v, ok := mr.MockGetStructured[key]; ok {
		json.Unmarshal(v, val)
	}
	return nil
}

func (mr *FakeMemcacheRing) GetMulti(serverKey string, keys []string) (map[string]interface{}, error) {
	return nil, nil
}

func (mr *FakeMemcacheRing) Incr(key string, delta int64, timeout int) (int64, error) {
	mr.MockIncrKeys = append(mr.MockIncrKeys, key)
	if len(mr.MockIncrResults) > 0 {
		res := mr.MockIncrResults[0] + delta
		mr.MockIncrResults = mr.MockIncrResults[1:]
		return res, nil
	}
	return int64(0), nil
}

func (mr *FakeMemcacheRing) Set(key string, value interface{}, timeout int) error {
	mr.MockSetValues = append(mr.MockSetValues, value)
	return nil
}

func (mr *FakeMemcacheRing) SetMulti(serverKey string, values map[string]interface{}, timeout int) error {
	return nil
}

type MockResponseWriter struct {
	SaveHeader *http.Header
	StatusMap  map[string]int
}

func (m MockResponseWriter) Header() (h http.Header) {
	if m.SaveHeader == nil {
		return http.Header{}
	}
	return *m.SaveHeader
}

func (m MockResponseWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (m MockResponseWriter) WriteString(s string) (n int, err error) {
	return len(s), nil
}

func (m MockResponseWriter) WriteHeader(s int) {
	if m.StatusMap != nil {
		m.StatusMap["S"] = s
	}
}
