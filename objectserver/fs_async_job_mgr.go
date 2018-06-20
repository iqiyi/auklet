package objectserver

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"

	"go.uber.org/zap"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/conf"
	"github.com/iqiyi/auklet/common/fs"
	"github.com/iqiyi/auklet/common/pickle"
)

const (
	ASYNC_JOB_DIR_PREFIX = "async_pending"
	ASYNC_JOB_BUF_SIZE   = 256
)

// I'm afraid we can't reuse here KVAsyncJob since FSAsyncJobMgr
// uses another serialization framework, pickle.
// Cannot figure out how to ignore field in Pickle. Both Device
// and Policy could be ignored.
type FSAsyncJob struct {
	Method    string            `pickle:"op"`
	Headers   map[string]string `pickle:"headers"`
	Account   string            `pickle:"account"`
	Container string            `pickle:"container"`
	Object    string            `pickle:"obj"`
	Device    string            `pickle:"device"`
	Policy    int               `pickle:"policy"`
}

func (j *FSAsyncJob) GetMethod() string {
	return j.Method
}

func (j *FSAsyncJob) GetAccount() string {
	return j.Account
}

func (j *FSAsyncJob) GetContainer() string {
	return j.Container
}

func (j *FSAsyncJob) GetObject() string {
	return j.Object
}

func (j *FSAsyncJob) GetHeaders() map[string]string {
	return j.Headers
}

type FSAsyncJobMgr struct {
	hashPrefix string
	hashSuffix string
	driveRoot  string
	jobs       map[string][]*FSAsyncJob
}

func (m *FSAsyncJobMgr) New(vars, headers map[string]string) AsyncJob {
	// We can ignore the error safely here
	p, _ := strconv.Atoi(vars["policy"])
	return &FSAsyncJob{
		Method:    vars["method"],
		Account:   vars["account"],
		Container: vars["container"],
		Object:    vars["object"],
		Device:    vars["device"],
		Headers:   headers,
		Policy:    p,
	}
}

func (m *FSAsyncJobMgr) asyncJobDir(policy int) string {
	suffix := ""
	if policy != 0 {
		suffix = fmt.Sprintf("-%d", policy)
	}

	return fmt.Sprintf("%s%s", ASYNC_JOB_DIR_PREFIX, suffix)
}

func (m *FSAsyncJobMgr) asyncJobPath(job *FSAsyncJob) string {
	hash := common.HashObjectName(
		m.hashPrefix, job.Account, job.Container, job.Object, m.hashSuffix)
	name := fmt.Sprintf("%s-%s", hash, job.Headers[common.XTimestamp])
	return filepath.Join(
		m.driveRoot, job.Device, m.asyncJobDir(job.Policy), hash[29:32], name)
}

func (m *FSAsyncJobMgr) Save(job AsyncJob) error {
	j := job.(*FSAsyncJob)
	p := m.asyncJobPath(j)
	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0755); err != nil {
		glogger.Error("unable to create dir for async job",
			zap.String("path", dir), zap.Error(err))
		return err
	}
	t := fs.TempDir(m.driveRoot, j.Device)
	w, err := fs.NewAtomicFileWriter(t, dir)
	if err != nil {
		glogger.Error("unable to create afw for async job", zap.Error(err))
		return err
	}

	defer w.Abandon()
	if _, err := w.Write(pickle.PickleDumps(j)); err != nil {
		glogger.Error("unable to write async job", zap.Error(err))
		return err
	}

	return w.Save(p)
}

func (m *FSAsyncJobMgr) listAsyncJobs(
	device string, policy, num int) []*FSAsyncJob {
	p := filepath.Join(
		m.driveRoot, device, m.asyncJobDir(policy), "[a-f0-9][a-f0-9][a-f0-9]")
	dirs, err := filepath.Glob(p)
	if err != nil {
		glogger.Error("unable to list suffixes", zap.String("path", p))
		return nil
	}
	rand.Shuffle(len(dirs), func(i, j int) {
		dirs[i], dirs[j] = dirs[j], dirs[i]
	})

	var jobs []*FSAsyncJob
	for _, d := range dirs {
		list, err := fs.ReadDirNames(d)
		if err != nil {
			glogger.Error("unable to list suffix dir",
				zap.String("path", d), zap.Error(err))
			continue
		}

		for _, j := range list {
			b, err := ioutil.ReadFile(filepath.Join(d, j))
			if err != nil {
				glogger.Error("unable to read async job file",
					zap.String("path", j), zap.Error(err))
				continue
			}

			aj := new(FSAsyncJob)
			if err := pickle.Unmarshal(b, &aj); err != nil {
				glogger.Error("unable to unmarshal async job",
					zap.String("path", j), zap.Error(err))
				continue
			}

			aj.Device = device
			aj.Policy = policy

			jobs = append(jobs, aj)
			if len(jobs) >= num {
				return jobs
			}
		}
	}

	return jobs
}

func (m *FSAsyncJobMgr) Next(device string, policy int) AsyncJob {
	idx := fmt.Sprintf("%s-%d", device, policy)
	buf := m.jobs[idx]
	if len(buf) == 0 {
		buf = m.listAsyncJobs(device, policy, ASYNC_JOB_BUF_SIZE)
	}

	if len(buf) == 0 {
		return nil
	}

	next := buf[0]
	buf = buf[1:]
	m.jobs[idx] = buf

	return next
}

func (m *FSAsyncJobMgr) Finish(job AsyncJob) error {
	j := job.(*FSAsyncJob)
	p := m.asyncJobPath(j)

	if err := os.Remove(p); err != nil {
		glogger.Error("unable to remove async job file",
			zap.String("path", p), zap.Error(err))
		return err
	}

	// If there is any other entry, directory not empty error will be raised.
	// So we simply ignore the error here
	os.Remove(filepath.Dir(p))

	return nil
}

func NewFSAsyncJobMgr(driveRoot string) (*FSAsyncJobMgr, error) {
	mgr := &FSAsyncJobMgr{
		driveRoot: driveRoot,
		jobs:      make(map[string][]*FSAsyncJob),
	}

	var err error
	mgr.hashPrefix, mgr.hashSuffix, err = conf.GetHashPrefixAndSuffix()
	if err != nil {
		glogger.Error("unable to find hash prefix/suffix", zap.Error(err))
		return nil, err
	}

	return mgr, nil
}
