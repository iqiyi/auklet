package objectserver

import (
	"flag"

	"github.com/iqiyi/auklet/common/conf"
)

const (
	BLOOMFILTER_ENTRIES  = 8096.0
	BLOOMFILTER_FP_RATIO = 0.01
)

type AsyncJob interface {
	GetMethod() string
	GetHeaders() map[string]string
	GetAccount() string
	GetContainer() string
	GetObject() string
}

type AsyncJobMgr interface {
	New(vars, headers map[string]string) AsyncJob

	Save(job AsyncJob) error

	Next(device string, policy int) AsyncJob

	Finish(job AsyncJob) error
}

func initKVAsyncJobMgr(
	cnf conf.Config, flags *flag.FlagSet) (*KVAsyncJobMgr, error) {
	port := int(cnf.GetInt("app:object-server", "async_kv_service_port", 60001))
	return NewKVAsyncJobMgr(port)
}

func initFSAsyncJobMgr(
	cnf conf.Config, flags *flag.FlagSet) (*FSAsyncJobMgr, error) {
	driveRoot := cnf.GetDefault("app:object-server", "devices", "/srv/node")
	return NewFSAsyncJobMgr(driveRoot)
}

func NewAsyncJobMgr(cnf conf.Config, flags *flag.FlagSet) (AsyncJobMgr, error) {
	mgr := cnf.GetDefault("app:object-server", "async_job_manager", "fs")
	switch mgr {
	case "fs":
		return initFSAsyncJobMgr(cnf, flags)
	case "kv":
		return initKVAsyncJobMgr(cnf, flags)
	default:
		return nil, ErrUnknownAsyncJobMgr
	}
}
