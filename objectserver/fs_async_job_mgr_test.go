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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFSMgrSaveJob(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	mgr, err := NewFSAsyncJobMgr(root)
	require.Nil(t, err)

	job := newFSAsyncJob()
	require.Nil(t, mgr.Save(job))
}

func TestFSMgrListJobs1(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	mgr, err := NewFSAsyncJobMgr(root)
	require.Nil(t, err)

	job := newFSAsyncJob()
	mgr.Save(job)

	require.Equal(t, job, mgr.Next(job.Device, int(job.Policy)))
}

func TestFSMgrListJobs2(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	mgr, _ := NewFSAsyncJobMgr(root)

	job1 := newFSAsyncJob()
	job2 := newFSAsyncJob()
	job2.Device = job1.Device
	job2.Policy = job1.Policy
	mgr.Save(job1)
	mgr.Save(job2)

	var jobs []AsyncJob
	j := mgr.Next(job1.Device, int(job1.Policy))
	for ; j != nil; j = mgr.Next(job1.Device, int(job1.Policy)) {
		jobs = append(jobs, j)
	}

	require.Len(t, jobs, 2)
}

func TestFSMgrFinishJobs(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	mgr, _ := NewFSAsyncJobMgr(root)

	job1 := newFSAsyncJob()
	job2 := newFSAsyncJob()
	job2.Device = job1.Device
	job2.Policy = job1.Policy
	mgr.Save(job1)
	mgr.Save(job2)

	require.Nil(t, mgr.Finish(job2))
	var jobs []AsyncJob
	j := mgr.Next(job1.Device, int(job1.Policy))
	for ; j != nil; j = mgr.Next(job1.Device, int(job1.Policy)) {
		jobs = append(jobs, j)
	}

	require.Len(t, jobs, 1)
	require.Equal(t, job1, jobs[0])
}
