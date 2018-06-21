package objectserver

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/iqiyi/auklet/common"
)

func TestKVMgrSaveJob(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	s := NewKVStore(root, 0)
	s.setTestMode(true)

	port := common.RandIntInRange(50001, 60000)
	svc := NewKVService(s, port)
	go svc.start()
	time.Sleep(time.Millisecond * 10)
	defer svc.stop()

	mgr, err := NewKVAsyncJobMgr(port)
	require.Nil(t, err)

	job := newKVAsyncJob()
	require.Nil(t, mgr.Save(job))
}

func TestKVMgrListJobs1(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	s := NewKVStore(root, 0)
	s.setTestMode(true)

	port := common.RandIntInRange(50001, 60000)
	svc := NewKVService(s, port)
	go svc.start()
	time.Sleep(time.Millisecond * 10)
	defer svc.stop()

	mgr, err := NewKVAsyncJobMgr(port)
	require.Nil(t, err)

	job := newKVAsyncJob()
	mgr.Save(job)

	require.Equal(t, job, mgr.Next(job.Device, int(job.Policy)))
}

func TestKVMgrListJobs2(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	s := NewKVStore(root, 0)
	s.setTestMode(true)

	port := common.RandIntInRange(50001, 60000)
	svc := NewKVService(s, port)
	go svc.start()
	time.Sleep(time.Millisecond * 10)
	defer svc.stop()

	mgr, err := NewKVAsyncJobMgr(port)
	require.Nil(t, err)

	job1 := newKVAsyncJob()
	job2 := newKVAsyncJob()
	mgr.Save(job1)
	mgr.Save(job2)

	var jobs []AsyncJob
	j := mgr.Next(job1.Device, int(job1.Policy))
	for ; j != nil; j = mgr.Next(job1.Device, int(job1.Policy)) {
		jobs = append(jobs, j)
	}

	expected := []AsyncJob{job1, job2}
	expctedEqual(t, expected, jobs)
}

func TestKVMgrFinishJobs(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	s := NewKVStore(root, 0)
	s.setTestMode(true)

	port := common.RandIntInRange(50001, 60000)
	svc := NewKVService(s, port)
	go svc.start()
	time.Sleep(time.Millisecond * 10)
	defer svc.stop()

	mgr, err := NewKVAsyncJobMgr(port)
	require.Nil(t, err)

	job1 := newKVAsyncJob()
	job2 := newKVAsyncJob()
	mgr.Save(job1)
	mgr.Save(job2)

	require.Nil(t, mgr.Finish(job2))

	var jobs []AsyncJob
	j := mgr.Next(job1.Device, int(job1.Policy))
	for ; j != nil; j = mgr.Next(job1.Device, int(job1.Policy)) {
		jobs = append(jobs, j)
	}

	expected := []AsyncJob{job1}
	expctedEqual(t, expected, jobs)
}
