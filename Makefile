# Copyright (c) 2015 Rackspace
# Copyright (c) 2016-2018 iQIYI.com.  All rights reserved.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
AUKLET_VERSION?=$(shell git describe --tags)

all: bin/auklet

all_debug:
	mkdir -p bin
	go build -o bin/auklet -gcflags='all=-N -l' -ldflags "-X github.com/iqiyi/auklet/common.Version=$(AUKLET_VERSION)" github.com/iqiyi/auklet/cmd/auklet

bin/auklet: */*.go */*/*.go
	mkdir -p bin
	go build -o bin/auklet -ldflags "-X github.com/iqiyi/auklet/common.Version=$(AUKLET_VERSION)" github.com/iqiyi/auklet/cmd/auklet

get:
	go get -t $(shell go list ./... | grep -v /vendor/)

fmt:
	gofmt -l -w -s $(shell find . -mindepth 1 -maxdepth 1 -type d -print | grep -v vendor)

test:
	@test -z "$(shell find . -name '*.go' | grep -v ./vendor/ | xargs gofmt -l -s)" || (echo "You need to run 'make fmt'"; exit 1)
	go vet $(shell go list ./... | grep -v /vendor/)
	go test -cover $(shell go list ./... | grep -v /vendor/)

install: bin/auklet
	cp bin/auklet $(DESTDIR)/usr/bin/auklet

develop: bin/auklet
	ln -f -s bin/auklet /usr/local/bin/auklet
