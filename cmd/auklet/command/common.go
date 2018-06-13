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

package command

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/iqiyi/auklet/common/conf"
)

const (
	EXIT_OK = iota
	EXIT_USAGE
	EXIT_START
	EXIT_STOP
	EXIT_RESTART
	EXIT_ERROR
)

var configFiles = map[string]string{
	"object":          "object",
	"pack-auditor":    "object",
	"pack-replicator": "object",
}

func findProcess(name string) (*os.Process, error) {
	var pid int
	file, err := os.Open(filepath.Join(conf.RunDir, fmt.Sprintf("%s.pid", name)))
	if err != nil {
		return nil, err
	}
	_, err = fmt.Fscanf(file, "%d", &pid)
	if err != nil {
		return nil, err
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return nil, err
	}
	err = process.Signal(syscall.Signal(0))
	if err != nil {
		return nil, err
	}
	return process, nil
}

func startServer(name string, args ...string) error {
	process, err := findProcess(name)
	if err == nil {
		process.Release()
		return ServerAlreadyRunning
	}

	serverConf := conf.FindServerConfig(configFiles[name])
	if serverConf == "" {
		return ServerConfNotFound
	}

	serverExecutable, err := exec.LookPath(os.Args[0])
	if err != nil {
		return err
	}

	uid, gid, err := conf.UidFromConf(serverConf)
	if err != nil {
		return err
	}

	cmd := exec.Command(serverExecutable, append([]string{name, "-c", serverConf}, args...)...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if uint32(os.Getuid()) != uid {
		cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uid, Gid: gid}
	}
	cmd.Stdin = nil

	syscall.Umask(022)
	err = cmd.Start()
	if err != nil {
		return err
	}
	file, err := os.Create(filepath.Join(conf.RunDir, fmt.Sprintf("%s.pid", name)))
	if err != nil {
		return err
	}
	defer file.Close()

	fmt.Fprintf(file, "%d", cmd.Process.Pid)
	return nil
}

func killServer(name string, sig os.Signal) error {
	process, err := findProcess(name)
	if err != nil {
		return err
	}
	process.Signal(sig)
	process.Wait()
	os.Remove(filepath.Join(conf.RunDir, fmt.Sprintf("%s.pid", name)))
	return nil
}

func stopServer(name string, args ...string) error {
	return killServer(name, syscall.SIGKILL)
}

func shutdownServer(name string, args ...string) error {
	return killServer(name, syscall.SIGTERM)
}
