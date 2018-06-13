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

package ring

import (
	"fmt"
)

type Device struct {
	Id              int     `json:"id"`
	Device          string  `json:"device"`
	Ip              string  `json:"ip"`
	Meta            string  `json:"meta"`
	Port            int     `json:"port"`
	Region          int     `json:"region"`
	ReplicationIp   string  `json:"replication_ip"`
	ReplicationPort int     `json:"replication_port"`
	Weight          float64 `json:"weight"`
	Zone            int     `json:"zone"`
}

func (d *Device) String() string {
	return fmt.Sprintf("Device{Id: %d, Device: %s, Ip: %s, Port: %d}",
		d.Id, d.Device, d.Ip, d.Port)
}
