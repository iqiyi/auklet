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
	"errors"
)

// Pack engine errors
var (
	ErrLockPath                  = errors.New("Error locking path")
	ErrPathNotDir                = errors.New("Path is not a directory")
	ErrNeedleNotAligned          = errors.New("needle offset not aligned")
	ErrNotImplementedErr         = errors.New("not implemented yet")
	ErrDataNotEmpty              = errors.New("data must be empty in meta needle")
	ErrPackDeviceManagerNotFound = errors.New("pack device manager not found")
	ErrPackDeviceNotFound        = errors.New("pack device not found")
	ErrPartitionNotFound         = errors.New("partition not found")
	ErrEmptyPartition            = errors.New("partition is empty")
	ErrDBIndexCorrupted          = errors.New("data in RocksDB can not be deserialized")
	ErrMalformedPickleFile       = errors.New("pickle file is malformed")
	ErrMalformedData             = errors.New("pickle data is malformed")
	ErrObjectNotDelete           = errors.New("unable to delete remote object")
	ErrObjectDataNotSynced       = errors.New("unable to sync object data")
	ErrObjectMetaNotSynced       = errors.New("unable to sync object meta")
	ErrBundleModifiedErr         = errors.New("bundle has been modified")
	ErrObjectsDiff               = errors.New("unable to diff objects with remotes")
	ErrWrongDeallocaion          = errors.New("deallocation on wrong type")
	ErrMetaNotLoaded             = errors.New("unable to load meta data")
	ErrNotPackEngine             = errors.New("engine is not pack type")
	ErrWrongDataWriter           = errors.New("data writer is not correct type")
	ErrRemoteDiskUnmounted       = errors.New("remote disk is unmounted")
	ErrRemoteHash                = errors.New("unable to get remote hash")
	ErrHashConfNotFound          = errors.New("unable to read hash prefix and suffxi")
)
