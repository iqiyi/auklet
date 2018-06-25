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
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iqiyi/auklet/common"
)

func TestKVStoreTestMode(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	s := NewKVStore(root, 0)
	db := s.getDB(TEST_DEVICE)
	require.Nil(t, db)

	s.testMode = true
	db = s.getDB(TEST_DEVICE)
	require.NotNil(t, db)
}

func TestAsyncJobPrefix(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	s := NewKVStore(root, 0)

	require.Equal(t, "/async_pending", s.asyncJobPrefix(0))
	require.Equal(t, "/async_pending-1", s.asyncJobPrefix(1))
}

func TestAsyncJobKey(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	s := NewKVStore(root, 0)

	s.hashPrefix = ""
	s.hashSuffix = "changeme"

	ts := "1529551760.123456"
	job := &KVAsyncJob{
		Method:    http.MethodPut,
		Account:   "a",
		Container: "c",
		Object:    "o",
		Headers:   map[string]string{common.XTimestamp: ts},
		Device:    TEST_DEVICE,
		Policy:    0,
	}

	expected := "/async_pending/099/2f714cd91b0e5d803cde2012b01d7099-1529551760.123456"
	require.Equal(t, expected, s.asyncJobKey(job))

	job.Policy = 1
	expected = "/async_pending-1/099/2f714cd91b0e5d803cde2012b01d7099-1529551760.123456"
	require.Equal(t, expected, s.asyncJobKey(job))
}

func TestSaveAsyncJob(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	s := NewKVStore(root, 0)
	s.setTestMode(true)

	job := newKVAsyncJob()
	require.Nil(t, s.SaveAsyncJob(job))
	jobs, _ := s.ListAsyncJobs(TEST_DEVICE, 0, KV_JOBS_PAGINATION)
	require.Equal(t, job, jobs[0])
}

func TestListAsyncJobs(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	s := NewKVStore(root, 0)
	s.setTestMode(true)

	job1 := newKVAsyncJob()
	job2 := newKVAsyncJob()

	s.SaveAsyncJob(job1)
	s.SaveAsyncJob(job2)

	jobs, err := s.ListAsyncJobs(TEST_DEVICE, 0, KV_JOBS_PAGINATION)
	require.Nil(t, err)

	expected := []AsyncJob{job1, job2}
	expctedEqual(t, expected, toGeneric(jobs))
}

func TestFinishAsyncJob(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	s := NewKVStore(root, 0)
	s.setTestMode(true)

	job := newKVAsyncJob()
	s.SaveAsyncJob(job)
	require.Nil(t, s.CleanAsyncJob(job))
	jobs, _ := s.ListAsyncJobs(TEST_DEVICE, 0, KV_JOBS_PAGINATION)
	require.Len(t, jobs, 0)
}
