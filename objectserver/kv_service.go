package objectserver

import (
	"fmt"
	"net"

	"go.uber.org/zap"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/iqiyi/auklet/common"
)

type KVService struct {
	kv   *KVStore
	port int
	srv  *grpc.Server
	fs   *FSStore
}

func (k *KVService) start() {
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", k.port))
	if err != nil {
		common.BootstrapLogger.Fatal("kv rpc server listen fail")
	}

	k.srv = grpc.NewServer()
	RegisterKVServiceServer(k.srv, k)
	k.srv.Serve(l)
}

func (k *KVService) stop() {
	k.srv.GracefulStop()
}

func (k *KVService) SaveAsyncJob(
	ctx context.Context, msg *SaveAsyncJobMsg) (*SaveAsyncJobReply, error) {
	err := k.kv.SaveAsyncJob(msg.Job)
	if err != nil {
		glogger.Error("unable to save async job", zap.Error(err))
	}

	return &SaveAsyncJobReply{Success: err == nil}, nil
}

func (k *KVService) convertFSJob(job *FSAsyncJob) *KVAsyncJob {
	return &KVAsyncJob{
		Method:    job.Method,
		Headers:   job.Headers,
		Account:   job.Account,
		Container: job.Container,
		Object:    job.Object,
		Device:    job.Device,
		Policy:    int32(job.Policy),
	}
}

func (k *KVService) listFSAsyncJobs(
	device string, policy int, num int) (*ListAsyncJobsReply, error) {
	jobs, err := k.fs.ListAsyncJobs(device, policy, num)
	reply := &ListAsyncJobsReply{}
	if err != nil {
		glogger.Error("unable to list fs async jobs", zap.Error(err))
		return reply, nil
	}

	reply.Jobs = make([]*KVAsyncJob, len(jobs))
	for i := range jobs {
		reply.Jobs[i] = k.convertFSJob(jobs[i])
	}

	return reply, nil
}

func (k *KVService) ListAsyncJobs(
	ctx context.Context, msg *ListAsyncJobsMsg) (*ListAsyncJobsReply, error) {
	reply := &ListAsyncJobsReply{}
	var err error
	reply.Jobs, err = k.kv.ListAsyncJobs(
		msg.Device, int(msg.Policy), int(msg.Pagination))
	if err != nil {
		glogger.Error("unable to list async jobs", zap.Error(err))
	}

	if len(reply.Jobs) == 0 && k.fs != nil {
		return k.listFSAsyncJobs(msg.Device, int(msg.Policy), int(msg.Pagination))
	}

	return reply, nil
}

func (k *KVService) CleanAsyncJob(
	ctx context.Context, msg *CleanAsyncJobMsg) (*CleanAsyncJobReply, error) {
	err := k.kv.CleanAsyncJob(msg.Job)
	if err != nil {
		glogger.Error("unable to clean async job", zap.Error(err))
	}

	return &CleanAsyncJobReply{Success: err == nil}, nil
}

func NewKVService(kv *KVStore, rpcPort int) *KVService {
	return &KVService{
		kv:   kv,
		port: rpcPort,
	}
}
func NewKVFSService(fs *FSStore, kv *KVStore, rpcPort int) *KVService {
	return &KVService{
		kv:   kv,
		port: rpcPort,
		fs:   fs,
	}
}
