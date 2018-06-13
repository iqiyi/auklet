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

func (m *ObjectMeta) DeepCopy() *ObjectMeta {
	meta := &ObjectMeta{
		Name:       m.Name,
		Timestamp:  m.Timestamp,
		DataSize:   m.DataSize,
		SystemMeta: make(map[string]string),
		UserMeta:   make(map[string]string),
	}

	for k, v := range m.SystemMeta {
		meta.SystemMeta[k] = v
	}

	for k, v := range m.UserMeta {
		meta.UserMeta[k] = v
	}

	return meta
}
