// Copyright (c) 2015 Rackspace
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

package fs

// AtomicFileWriter saves a new file atomically.
type AtomicFileWriter interface {
	// Write writes the data to the underlying file.
	Write([]byte) (int, error)
	// Fd returns the file's underlying file descriptor.
	Fd() uintptr
	// Save atomically writes the file to its destination.
	Save(string) error
	// Abandon removes any resources associated with this file.
	Abandon() error
	// Preallocate pre-allocates space on disk,
	// given the expected file size and disk reserve size.
	Preallocate(int64, int64) error
}
