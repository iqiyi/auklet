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
	"bytes"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/vaughan0/go-ini"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/fs"
)

// Config represents an ini file.
type Config struct{ ini.File }

type Section struct {
	ini.Section
	c       Config
	section string
}

// Get fetches a value from the Config, looking in the DEFAULT section
// if not found in the specific section.
// Also ignores "set " key prefixes, like paste.
func (f Config) Get(section string, key string) (string, bool) {
	if value, ok := f.File.Get(section, key); ok {
		return value, true
	}

	if value, ok := f.File.Get("DEFAULT", key); ok {
		return value, true
	}

	if value, ok := f.File.Get(section, "set "+key); ok {
		return value, true
	}

	if value, ok := f.File.Get("DEFAULT", "set "+key); ok {
		return value, true
	}

	return "", false
}

// GetDefault returns a value from the config, or returns the default
// setting if the entry doesn't exist.
func (f Config) GetDefault(section string, key string, dfl string) string {
	if value, ok := f.Get(section, key); ok {
		return value
	}

	return dfl
}

// GetBool loads a true/false value from the // config, with support
// for things like "yes", "true", "1", "t", etc.
func (f Config) GetBool(section string, key string, dfl bool) bool {
	if value, ok := f.Get(section, key); ok {
		return common.LooksTrue(value)
	}

	return dfl
}

// GetInt loads an entry from the config, parsed as an integer value.
func (f Config) GetInt(section string, key string, dfl int64) int64 {
	if value, ok := f.Get(section, key); ok {
		if val, err := strconv.ParseInt(value, 10, 64); err == nil {
			return val
		}
		panic(fmt.Sprintf("error parsing integer %s/%s from config.", section, key))
	}

	return dfl
}

// GetFloat loads an entry from the config, parsed as a floating point value.
func (f Config) GetFloat(section string, key string, dfl float64) float64 {
	if value, ok := f.Get(section, key); ok {
		if val, err := strconv.ParseFloat(value, 64); err == nil {
			return val
		}
		panic(fmt.Sprintf("Error parsing float %s/%s from config.", section, key))
	}

	return dfl
}

// GetLimit loads an entry from the config in the format of %d/%d.
func (f Config) GetLimit(
	section string, key string, dfla int64, dflb int64) (int64, int64) {

	if value, ok := f.Get(section, key); ok {
		fmt.Sscanf(value, "%d/%d", &dfla, &dflb)
	}

	return dfla, dflb
}

// HasSection determines whether or not the section exists in the ini file.
func (f Config) HasSection(section string) bool {
	return f.File[section] != nil
}

// GetSection returns a Section struct.
func (f Config) GetSection(section string) Section {
	return Section{f.File[section], f, section}
}

func (s Section) Get(key string) (string, bool) {
	return s.c.Get(s.section, key)
}

func (s Section) GetDefault(key string, dfl string) string {
	return s.c.GetDefault(s.section, key, dfl)
}

func (s Section) GetBool(key string, dfl bool) bool {
	return s.c.GetBool(s.section, key, dfl)
}

func (s Section) GetInt(key string, dfl int64) int64 {
	return s.c.GetInt(s.section, key, dfl)
}

func (s Section) GetFloat(key string, dfl float64) float64 {
	return s.c.GetFloat(s.section, key, dfl)
}

func (s Section) GetLimit(key string, dfla int64, dflb int64) (int64, int64) {
	return s.c.GetLimit(s.section, key, dfla, dflb)
}

// LoadConfig loads an ini from a path.
// The path should be a *.conf file or a *.conf.d directory.
func LoadConfig(path string) (Config, error) {
	file := Config{make(ini.File)}

	fi, err := os.Stat(path)
	if err != nil {
		return file, err
	}

	if fi.IsDir() {
		files, err := filepath.Glob(filepath.Join(path, "*.conf"))
		if err != nil {
			return file, err
		}

		sort.Strings(files)
		for _, subfile := range files {
			sf, err := LoadConfig(subfile)
			if err != nil {
				return file, err
			}
			for sec, val := range sf.File {
				file.File[sec] = val
			}
		}

		return file, nil
	}

	return file, file.LoadFile(path)
}

// LoadConfigs finds and loads any configs that exist for the given path.
// Multiple configs are supported for things like SAIO setups.
func LoadConfigs(path string) ([]Config, error) {
	configPaths := []string{}
	configs := []Config{}
	fi, err := os.Stat(path)
	if err == nil && fi.IsDir() && !strings.HasSuffix(path, ".conf.d") {
		multiConfigs, err := filepath.Glob(filepath.Join(path, "*.conf"))
		if err == nil {
			configPaths = append(configPaths, multiConfigs...)
		}

		multiConfigs, err = filepath.Glob(filepath.Join(path, "*.conf.d"))
		if err == nil {
			configPaths = append(configPaths, multiConfigs...)
		}
	} else {
		configPaths = append(configPaths, path)
	}

	for _, p := range configPaths {
		if config, err := LoadConfig(p); err == nil {
			configs = append(configs, config)
		}
	}

	if len(configs) == 0 {
		return nil, ErrConfNotFound
	}

	return configs, nil
}

// StringConfig returns an Config from a string, for use in tests.
func StringConfig(data string) (Config, error) {
	file := Config{make(ini.File)}
	return file, file.Load(bytes.NewBufferString(data))
}

// UidFromConf returns the uid and gid for the user set in the first
// config found.
func UidFromConf(path string) (uint32, uint32, error) {
	configs, err := LoadConfigs(path)
	if err != nil {
		return 0, 0, err
	}

	for _, config := range configs {
		username := config.GetDefault("DEFAULT", "user", "swift")
		usr, err := user.Lookup(username)
		if err != nil {
			return 0, 0, err
		}

		uid, err := strconv.ParseUint(usr.Uid, 10, 32)
		if err != nil {
			return 0, 0, err
		}

		gid, err := strconv.ParseUint(usr.Gid, 10, 32)
		if err != nil {
			return 0, 0, err
		}

		return uint32(uid), uint32(gid), nil
	}

	return 0, 0, ErrUidNotFound
}

func GetHashPrefixAndSuffix() (string, string, error) {

	for _, loc := range configLocations {
		if conf, e := LoadConfig(loc); e == nil {
			var ok bool
			prefix, _ := conf.Get("swift-hash", "swift_hash_path_prefix")
			suffix, ok := conf.Get("swift-hash", "swift_hash_path_suffix")
			if !ok {
				return "", "", ErrHashSuffixNotFound
			}

			return prefix, suffix, nil
		}
	}

	return "", "", ErrHashConfigNotFound
}

func ReadResellerOptions(conf Section,
	defaults map[string][]string) ([]string, map[string]map[string][]string) {

	resellerPrefixOpt := conf.GetDefault("reseller_prefix", "AUTH")
	s := []string{}
	for _, val := range strings.Split(resellerPrefixOpt, ",") {
		v := strings.TrimSpace(val)
		if v != "" {
			s = append(s, strings.TrimSpace(v))
		}
	}
	resellerPrefix := []string{}
	for _, prefix := range s {
		if prefix == "''" {
			prefix = ""
		}
		if prefix != "" && !strings.HasSuffix(prefix, "_") {
			prefix = prefix + "_"
		}
		if !common.StringInSlice(prefix, resellerPrefix) {
			resellerPrefix = append(resellerPrefix, prefix)
		}
	}
	if len(resellerPrefix) == 0 {
		resellerPrefix = append(resellerPrefix, "")
	}
	associatedOptions := make(map[string]map[string][]string)
	for _, prefix := range resellerPrefix {
		associatedOptions[prefix] = make(map[string][]string)
		for k, v := range defaults {
			associatedOptions[prefix][k] = v
		}
		for k, v := range ReadPrefixedOptions(conf, "", defaults) {
			associatedOptions[prefix][k] = v
		}
		prefix_name := "''"
		if prefix != "" {
			prefix_name = prefix
		}
		for k, v := range ReadPrefixedOptions(conf, prefix_name, defaults) {
			associatedOptions[prefix][k] = v
		}
	}
	return resellerPrefix, associatedOptions
}

func ReadPrefixedOptions(conf Section, prefixName string,
	defaults map[string][]string) map[string][]string {

	params := make(map[string][]string)
	for optionName := range defaults {
		if value, ok := conf.Get(fmt.Sprintf("%s%s", prefixName, optionName)); ok {
			params[optionName] = []string{}
			for _, role := range strings.Split(strings.ToLower(value), ",") {
				params[optionName] = append(params[optionName], strings.TrimSpace(role))
			}
		}
	}
	return params
}

func FindServerConfig(name string) string {
	configName := strings.Split(name, "-")[0]
	configSearch := []string{
		fmt.Sprintf("/etc/auklet/%s-server.conf", configName),
		fmt.Sprintf("/etc/auklet/%s-server.conf.d", configName),
		fmt.Sprintf("/etc/auklet/%s-server", configName),
		fmt.Sprintf("/etc/swift/%s-server.conf", configName),
		fmt.Sprintf("/etc/swift/%s-server.conf.d", configName),
		fmt.Sprintf("/etc/swift/%s-server", configName),
	}
	for _, config := range configSearch {
		if fs.Exists(config) {
			return config
		}
	}
	return ""
}
