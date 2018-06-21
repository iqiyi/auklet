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
