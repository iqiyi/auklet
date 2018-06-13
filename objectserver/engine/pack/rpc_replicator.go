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

// RPC API for object replication
package pack

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	context "golang.org/x/net/context"

	"github.com/iqiyi/auklet/common"
)

func (s *PackRpcServer) GetHashes(
	ctx context.Context, msg *SuffixHashesMsg) (*SuffixHashesReply, error) {
	device, err := s.getDevice(int(msg.Policy), msg.Device)
	if err != nil {
		return nil, err
	}

	hashed, hashes, err := device.GetHashes(
		msg.Partition, msg.Recalculate, msg.ListDir, common.ONE_WEEK)
	if err != nil {
		return nil, err
	}

	reply := &SuffixHashesReply{
		Hashed: hashed,
		Hashes: hashes,
	}

	return reply, nil
}

func (s *PackRpcServer) diffObjects(timestamps map[string]*ObjectTimestamps,
	msg *SyncMsg) (map[string]*WantedParts, error) {

	url := fmt.Sprintf("http://%s:%d/%s/%s",
		msg.Host, msg.Port, msg.Device, msg.Partition)
	b, err := proto.Marshal(&CheckedObjects{Objects: timestamps})
	if err != nil {
		glogger.Error("unable to marshal checked objects",
			zap.String("paritition", msg.Partition),
			zap.Error(err))
		return nil, err
	}

	req, err := http.NewRequest("DIFF", url, bytes.NewBuffer(b))
	if err != nil {
		glogger.Error("unable to create diff request",
			zap.String("url", url),
			zap.Error(err))
		return nil, err
	}
	req.Header.Set(common.XBackendPolicyIndex, strconv.Itoa(int(msg.Policy)))

	resp, err := s.client.Do(req)
	if err == nil {
		defer resp.Body.Close()
	}
	if err != nil || resp.StatusCode != http.StatusOK {
		glogger.Error("unable to diff objects",
			zap.String("url", url), zap.Error(err))
		return nil, ErrObjectsDiff
	}

	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		glogger.Error("unable to read diff response body",
			zap.String("url", url), zap.Error(err))
		return nil, err
	}

	wanted := new(WantedObjects)
	if err = proto.Unmarshal(b, wanted); err != nil {
		glogger.Error("unable to unmarshal diff response",
			zap.String("url", url),
			zap.ByteString("body", b),
			zap.Error(err))
		return nil, err
	}

	return wanted.Objects, nil
}

func (s *PackRpcServer) sendDelete(url string,
	policy int, obj *PackObject) error {

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		glogger.Error("unable to create DELETE request",
			zap.String("url", url),
			zap.Error(err))
		return err
	}
	req.Header.Set(common.XTimestamp, obj.meta.Timestamp)
	req.Header.Set(common.XBackendPolicyIndex, strconv.Itoa(policy))

	resp, err := s.client.Do(req)
	if err != nil {
		glogger.Error("unable to send DELETE request",
			zap.String("url", url),
			zap.Error(err))
		return err
	}
	defer resp.Body.Close()

	// Because we are replicating a deletion, if the status code is not 404,
	// it should be considered as success.
	if !(resp.StatusCode == http.StatusNoContent ||
		resp.StatusCode == http.StatusNotFound) {
		glogger.Error("unable to delete remote object",
			zap.String("url", url),
			zap.String("status", resp.Status))
		return ErrObjectNotDelete
	}

	return nil
}

func (s *PackRpcServer) syncData(url string, policy int, obj *PackObject) error {
	reader, err := obj.device.NewReader(obj)
	if err != nil {
		glogger.Error("unable to create object reader",
			zap.String("object", obj.meta.Name),
			zap.Error(err))
		return err
	}

	req, err := http.NewRequest(http.MethodPut, url, reader)
	if err != nil {
		glogger.Error("unable to create PUT request",
			zap.String("url", url),
			zap.Error(err))
		return err
	}
	req.Header.Set(common.XTimestamp, obj.dMeta.Timestamp)
	req.Header.Set(common.XBackendPolicyIndex, strconv.Itoa(policy))
	req.Header.Set(
		common.XBackendObjLength, strconv.FormatInt(obj.dMeta.DataSize, 10))

	// N.B. We don't use obj.meta here because we just want to replicate
	// the data part
	for k, v := range obj.dMeta.SystemMeta {
		req.Header.Set(k, v)
	}
	for k, v := range obj.dMeta.UserMeta {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		glogger.Error("unable to send PUT request",
			zap.String("url", url),
			zap.Error(err))
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		glogger.Error("unable to PUT remote object",
			zap.String("url", url),
			zap.String("status", resp.Status))
		return ErrObjectDataNotSynced
	}

	return nil
}

func (s *PackRpcServer) syncMeta(url string, policy int, obj *PackObject) error {
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set(common.XTimestamp, obj.mMeta.Timestamp)
	req.Header.Set(common.XBackendPolicyIndex, strconv.Itoa(policy))

	// N.B. We don't use obj.meta here because we just want to replicate
	// the meta part
	for k, v := range obj.mMeta.SystemMeta {
		req.Header.Set(k, v)
	}
	for k, v := range obj.mMeta.UserMeta {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		glogger.Error("unable to send POST request",
			zap.String("url", url),
			zap.Error(err))
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		glogger.Error("unable to replicate meta part",
			zap.String("url", url),
			zap.String("status", resp.Status))
		return ErrObjectMetaNotSynced
	}

	return nil
}

func (s *PackRpcServer) syncObjects(wanted map[string]*WantedParts, msg *SyncMsg) (map[string]string, error) {
	device, err := s.getDevice(int(msg.Policy), msg.LocalDevice)
	if err != nil {
		return nil, err
	}

	candidates := make(map[string]string)
	for h, w := range wanted {
		obj := &PackObject{
			key:       generateKeyFromHash(msg.Partition, h),
			device:    device,
			partition: msg.Partition,
		}

		err = device.LoadObjectMeta(obj)
		if err != nil {
			glogger.Error("unable to load metadata",
				zap.String("object-key", obj.key),
				zap.Error(err))
			return nil, err
		}

		url := fmt.Sprintf("http://%s:%d/%s/%s%s",
			msg.Host, msg.Port, msg.Device, msg.Partition, obj.meta.Name)

		if w.Data && !obj.exists && obj.meta != nil {
			err = s.sendDelete(url, int(msg.Policy), obj)
			if err != nil {
				glogger.Error("unable to replicate deleted object",
					zap.String("object", obj.name),
					zap.Error(err))
				return nil, err
			}
			candidates[h] = obj.meta.Timestamp
			continue
		}

		if w.Data {
			err = s.syncData(url, int(msg.Policy), obj)
			if err != nil {
				glogger.Error("unable to replicate data part",
					zap.String("object", obj.name),
					zap.Error(err))
				return nil, err
			}
			candidates[h] = obj.meta.Timestamp
		}

		if w.Meta && obj.mMeta != nil {
			err = s.syncMeta(url, int(msg.Policy), obj)
			if err != nil {
				glogger.Error("unable to replicate meta part",
					zap.String("object", obj.name),
					zap.Error(err))
				return nil, err
			}
			candidates[h] = obj.meta.Timestamp
		}
	}

	return candidates, nil
}

// A successful flag would cause handoff partition to be deleted,
// so this method should fail fast at any error.
// The sucessful flag will be return only when ALL the suffixes are
// replicated sucessfully.
func (s *PackRpcServer) Sync(ctx context.Context, msg *SyncMsg) (*SyncReply, error) {
	// Sync use bool flag to indicate the call is successful or not.
	// So any possible expected error will be ignore when returning the call.
	reply := &SyncReply{
		Candidates: make(map[string]string),
	}

	device, err := s.getDevice(int(msg.Policy), msg.LocalDevice)
	if err != nil {
		return reply, nil
	}

	timestamps := make(map[string]*ObjectTimestamps)

	for _, suffix := range msg.Suffixes {
		tses, err := device.ListSuffixTimestamps(msg.Partition, suffix)
		if err != nil {
			glogger.Error("unable to list timestamps under suffix",
				zap.String("partition", msg.Partition),
				zap.String("suffix", suffix),
				zap.Error(err))
			return reply, nil
		}

		for h, ts := range tses {
			timestamps[h] = ts
		}
	}

	wanted, err := s.diffObjects(timestamps, msg)
	if err != nil {
		glogger.Error("unable to diff objects under suffix",
			zap.String("partition", msg.Partition),
			zap.Error(err))
		return reply, nil
	}

	reply.Candidates, err = s.syncObjects(wanted, msg)

	if err == nil {
		reply.Success = true
	}

	return reply, nil
}

func (s *PackRpcServer) DeleteHandoff(ctx context.Context, msg *Partition) (
	*PartitionDeletionReply, error) {
	device, err := s.getDevice(int(msg.Policy), msg.Device)
	if err != nil {
		glogger.Error("unable to get pack device",
			zap.String("device", msg.Device),
			zap.Error(err))
		return nil, err
	}

	err = device.DeleteHandoff(msg.Partition)
	reply := &PartitionDeletionReply{
		Success: err == nil,
	}

	return reply, err
}
