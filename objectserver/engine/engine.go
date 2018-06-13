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

package engine

import (
	"flag"
	"sync"

	"github.com/iqiyi/auklet/common/conf"
)

type ObjectEngine interface {
	New(vars map[string]string, needData bool) (Object, error)

	GetHashes(device, partition string, recalculate []string) (map[string]string, error)

	Close() error
}

type ObjectEngineConstructor func(conf.Config, *conf.Policy, *flag.FlagSet, *sync.WaitGroup) (ObjectEngine, error)

type engineFactoryEntry struct {
	name        string
	constructor ObjectEngineConstructor
}

var engineFactories = []engineFactoryEntry{}

func RegisterObjectEngine(name string, newEngine ObjectEngineConstructor) {
	for _, e := range engineFactories {
		if e.name == name {
			e.constructor = newEngine
			return
		}
	}
	engineFactories = append(engineFactories, engineFactoryEntry{name, newEngine})
}

func FindEngine(name string) (ObjectEngineConstructor, error) {
	for _, e := range engineFactories {
		if e.name == name {
			return e.constructor, nil
		}
	}
	return nil, ErrEngineNotFound
}
