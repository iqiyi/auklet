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
	store *KVStore
	port  int
	srv   *grpc.Server
}

func NewKVService(store *KVStore, port int) *KVService {
	return &KVService{
		store: store,
		port:  port,
	}
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
	err := k.store.SaveAsyncJob(msg.Job)
	if err != nil {
		glogger.Error("unable to save async job", zap.Error(err))
	}

	return &SaveAsyncJobReply{Success: err == nil}, nil
}

func (k *KVService) ListAsyncJobs(
	ctx context.Context, msg *ListAsyncJobsMsg) (*ListAsyncJobsReply, error) {
	reply := &ListAsyncJobsReply{}
	var err error
	reply.Jobs, err = k.store.ListAsyncJobs(
		msg.Device, int(msg.Policy), msg.Position, int(msg.Pagination))
	if err != nil {
		glogger.Error("unable to list async jobs", zap.Error(err))
	}

	return reply, nil
}

func (k *KVService) CleanAsyncJob(
	ctx context.Context, msg *CleanAsyncJobMsg) (*CleanAsyncJobReply, error) {
	err := k.store.CleanAsyncJob(msg.Job)
	if err != nil {
		glogger.Error("unable to clean async job", zap.Error(err))
	}

	return &CleanAsyncJobReply{Success: err == nil}, nil
}
