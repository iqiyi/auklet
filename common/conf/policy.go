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

package conf

import (
	"fmt"
	"strings"
)

type Policy struct {
	Index      int
	Type       string
	Name       string
	Aliases    []string
	Default    bool
	Deprecated bool
	Config     map[string]string
}

func LoadPolicies() map[int]*Policy {
	policies := map[int]*Policy{
		0: {
			Index:      0,
			Type:       "replication",
			Name:       "Policy-0",
			Aliases:    nil,
			Default:    false,
			Deprecated: false,
		},
	}
	for _, loc := range configLocations {
		conf, e := LoadConfig(loc)
		if e != nil {
			continue
		}

		for key := range conf.File {
			var policyIndex int
			c, err := fmt.Sscanf(key, "storage-policy:%d", &policyIndex)
			if err == nil && c == 1 {
				name := conf.GetDefault(
					key, "name", fmt.Sprintf("Policy-%d", policyIndex))
				aliases := []string{name}
				aliasList := conf.GetDefault(key, "aliases", "")
				for _, alias := range strings.Split(aliasList, ",") {
					alias = strings.Trim(alias, " ")
					if alias != "" && alias != name {
						aliases = append(aliases, alias)
					}
				}
				policies[policyIndex] = &Policy{
					Index:      policyIndex,
					Type:       conf.GetDefault(key, "policy_type", "replication"),
					Name:       name,
					Aliases:    aliases,
					Deprecated: conf.GetBool(key, "deprecated", false),
					Default:    conf.GetBool(key, "default", false),
					Config:     map[string]string(conf.File[key]),
				}
			}
		}

		break
	}

	defaultFound := false
	for _, policy := range policies {
		if policy.Default {
			defaultFound = true
		}
	}

	if !defaultFound {
		policies[0].Default = true
	}

	return policies
}
