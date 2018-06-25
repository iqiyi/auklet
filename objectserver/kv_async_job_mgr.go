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
	"fmt"
	"strconv"

	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	KV_JOBS_PAGINATION = 1024
)

type KVAsyncJobMgr struct {
	rpc  KVServiceClient
	jobs map[string][]*KVAsyncJob
}

func (m *KVAsyncJobMgr) New(vars, headers map[string]string) AsyncJob {
	// We can ignore the error safely here
	p, _ := strconv.Atoi(vars["policy"])
	return &KVAsyncJob{
		Method:    vars["method"],
		Account:   vars["account"],
		Container: vars["container"],
		Object:    vars["object"],
		Device:    vars["device"],
		Headers:   headers,
		Policy:    int32(p),
	}
}

func (m *KVAsyncJobMgr) Save(job AsyncJob) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	msg := &SaveAsyncJobMsg{job.(*KVAsyncJob)}
	reply, err := m.rpc.SaveAsyncJob(ctx, msg)
	if err != nil {
		return err
	}

	if !reply.Success {
		err = ErrKVAsyncJobNotSaved
	}

	return err
}

func (m *KVAsyncJobMgr) Next(device string, policy int) AsyncJob {
	idx := fmt.Sprintf("%s-%d", device, policy)
	buf := m.jobs[idx]
	if len(buf) == 0 {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		msg := &ListAsyncJobsMsg{
			Device:     device,
			Policy:     int32(policy),
			Pagination: KV_JOBS_PAGINATION,
		}
		reply, err := m.rpc.ListAsyncJobs(ctx, msg)
		if err != nil {
			glogger.Error("unable to list kv async jobs", zap.Error(err))
			return nil
		}
		buf = reply.Jobs
	}

	if len(buf) == 0 {
		return nil
	}

	next := buf[0]
	buf = buf[1:]
	m.jobs[idx] = buf

	return next
}

func (m *KVAsyncJobMgr) Finish(job AsyncJob) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	msg := &CleanAsyncJobMsg{job.(*KVAsyncJob)}

	reply, err := m.rpc.CleanAsyncJob(ctx, msg)
	if err != nil {
		return err
	}

	if !reply.Success {
		err = ErrKVAsyncJobNotClean
	}

	return err
}

func NewKVAsyncJobMgr(port int) (*KVAsyncJobMgr, error) {
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", port), grpc.WithInsecure())
	if err != nil {
		glogger.Error("unable to dial to rpc server",
			zap.Int("port", port), zap.Error(err))
		return nil, err
	}

	mgr := &KVAsyncJobMgr{
		rpc:  NewKVServiceClient(conn),
		jobs: make(map[string][]*KVAsyncJob),
	}

	return mgr, nil
}
