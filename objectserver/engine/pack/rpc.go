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
// 

package pack

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
)

// By design, there should be only one rpc server per host.
// But in SAIO, multiple hosts will be emulated in only one host.
// Here we use a map to hold all the rpc servers which are identified
// by object server's port.
var rpcServers map[int]*PackRpcServer

func init() {
	rpcServers = make(map[int]*PackRpcServer)
}

type PackRpcServer struct {
	port int

	bdms map[int]*PackDeviceMgr
	lock sync.RWMutex

	client *http.Client
}

func NewRpcServer(port int) *PackRpcServer {
	s := &PackRpcServer{
		port:   port,
		bdms:   make(map[int]*PackDeviceMgr),
		client: &http.Client{Timeout: 5 * time.Minute},
	}

	return s
}

func (s *PackRpcServer) RegisterPackDeviceMgr(bdm *PackDeviceMgr) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.bdms[bdm.Policy] = bdm
}

func (s *PackRpcServer) start() {
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", s.port))
	if err != nil {
		log.Fatalf("pack rpc server listen fail.")
	}

	server := grpc.NewServer()
	RegisterPackRpcServiceServer(server, s)
	server.Serve(listener)
}

func (s *PackRpcServer) getDevice(
	policy int, device string) (*PackDevice, error) {
	// Pack device manager must be registered during initialization, thus
	// there is no need to acquire the lock.
	bdm := s.bdms[policy]
	if bdm == nil {
		glogger.Error("pack device manager for policy not found",
			zap.Int("policy", policy))
		return nil, ErrPackDeviceManagerNotFound
	}
	d := bdm.GetPackDevice(device)
	if d == nil {
		glogger.Error("pack device not found",
			zap.Int("policy", policy), zap.String("device", device))
		return nil, ErrPackDeviceNotFound
	}

	return d, nil
}

func (s *PackRpcServer) ListPartitionSuffixes(
	ctx context.Context, msg *Partition) (*PartitionSuffixesReply, error) {

	device, err := s.getDevice(int(msg.Policy), msg.Device)
	if err != nil {
		return nil, err
	}

	reply := &PartitionSuffixesReply{
		Suffixes: device.ListSuffixes(msg.Partition),
	}
	return reply, nil
}

func (s *PackRpcServer) AuditPartition(
	ctx context.Context, msg *Partition) (*PartitionAuditionReply, error) {
	device, err := s.getDevice(int(msg.Policy), msg.Device)
	if err != nil {
		return nil, err
	}

	stat, err := device.AuditPartition(msg.Partition)
	if err != nil {
		return nil, err
	}

	reply := &PartitionAuditionReply{
		ProcessedBytes: stat.ProcessedBytes,
		ProcessedFiles: stat.ProcessedFiles,
		Errors:         stat.Errors,
		Quarantines:    stat.Quarantines,
	}

	return reply, nil
}
