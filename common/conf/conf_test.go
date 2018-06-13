// Copyright (c) 2015 Rackspace
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
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	tempFile, err := ioutil.TempFile("", "INI")
	require.Nil(t, err)
	defer os.RemoveAll(tempFile.Name())
	tempFile.WriteString("[DEFAULT]\ndefvalue=1\n[stuff]\ntruevalue=true\nfalsevalue=false\nintvalue=3\nset log_facility = LOG_LOCAL1\nlimit=3/5\nfloatvalue=0.5\n")
	iniFile, err := LoadConfig(tempFile.Name())
	require.Equal(t, true, iniFile.GetBool("stuff", "truevalue", false))
	require.Equal(t, false, iniFile.GetBool("stuff", "falsevalue", true))
	require.Equal(t, true, iniFile.GetBool("stuff", "defaultvalue", true))
	require.Equal(t, int64(3), iniFile.GetInt("stuff", "intvalue", 2))
	require.Equal(t, int64(2), iniFile.GetInt("stuff", "missingvalue", 2))
	require.Equal(t, "false", iniFile.GetDefault("stuff", "falsevalue", "true"))
	require.Equal(t, "true", iniFile.GetDefault("stuff", "missingvalue", "true"))
	require.Equal(t, "LOG_LOCAL1", iniFile.GetDefault("stuff", "log_facility", "LOG_LOCAL0"))
	require.Equal(t, int64(1), iniFile.GetInt("stuff", "defvalue", 0))
	require.Equal(t, float64(0.5), iniFile.GetFloat("stuff", "floatvalue", 0))
	limit1, limit2 := iniFile.GetLimit("stuff", "limit", 1, 1)
	require.Equal(t, limit1, int64(3))
	require.Equal(t, limit2, int64(5))
}

func TestConfD(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(tempDir)
	ioutil.WriteFile(filepath.Join(tempDir, "0.conf"), []byte("[stuff]\ntruevalue=true\n[otherstuff]\nintvalue=3\n"), 0666)
	ioutil.WriteFile(filepath.Join(tempDir, "1.conf"), []byte("[stuff]\nfalsevalue=false\n"), 0666)
	iniFile, err := LoadConfig(tempDir)
	require.Nil(t, err)
	require.Equal(t, false, iniFile.GetBool("stuff", "falsevalue", true))      // falsevalue was set by later conf
	require.Equal(t, false, iniFile.GetBool("stuff", "truevalue", false))      // truevalue from earlier conf was unset
	require.Equal(t, int(3), int(iniFile.GetInt("otherstuff", "intvalue", 0))) // otherstuff from earlier conf was preserved
}

func TestLoadConfigs(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(tempDir)
	require.Nil(t, os.MkdirAll(filepath.Join(tempDir, "etcswift1"), 0755))
	ioutil.WriteFile(filepath.Join(tempDir, "etcswift1", "object-server.conf"), []byte("[stuff]\ntruevalue=true\n"), 0666)
	require.Nil(t, os.MkdirAll(filepath.Join(tempDir, "etcswift2", "object-server.conf.d"), 0755))
	ioutil.WriteFile(filepath.Join(tempDir, "etcswift2", "object-server.conf.d", "default.conf"), []byte("[stuff]\ntruevalue=true\n"), 0666)
	require.Nil(t, os.MkdirAll(filepath.Join(tempDir, "etcswift3", "object-server", "2.conf.d"), 0755))
	ioutil.WriteFile(filepath.Join(tempDir, "etcswift3", "object-server", "1.conf"), []byte("[stuff]\ntruevalue=true\n"), 0666)
	ioutil.WriteFile(filepath.Join(tempDir, "etcswift3", "object-server", "2.conf.d", "default.conf"), []byte("[stuff]\ntruevalue=true\n"), 0666)

	for _, configPath := range []string{
		filepath.Join(tempDir, "etcswift1", "object-server.conf"),
		filepath.Join(tempDir, "etcswift2", "object-server.conf.d"),
		filepath.Join(tempDir, "etcswift3", "object-server"),
	} {
		configs, err := LoadConfigs(configPath)
		require.Nil(t, err)
		for _, config := range configs {
			require.Equal(t, true, config.GetBool("stuff", "truevalue", false))
		}
	}
}

func TestUidFromConf(t *testing.T) {
	usr, err := user.Current()
	require.Nil(t, err)
	tempFile, err := ioutil.TempFile("", "INI")
	require.Nil(t, err)
	defer os.RemoveAll(tempFile.Name())
	defer tempFile.Close()
	fmt.Fprintf(tempFile, "[DEFAULT]\nuser=%s\n", usr.Username)

	currentUid, err := strconv.ParseUint(usr.Uid, 10, 32)
	require.Nil(t, err)
	currentGid, err := strconv.ParseUint(usr.Gid, 10, 32)
	require.Nil(t, err)
	uid, gid, err := UidFromConf(tempFile.Name())
	require.Nil(t, err)
	require.Equal(t, uint32(currentUid), uint32(uid))
	require.Equal(t, uint32(currentGid), uint32(gid))
}

func TestUidFromConfFailure(t *testing.T) {
	tempFile, err := ioutil.TempFile("", "INI")
	require.Nil(t, err)
	defer os.RemoveAll(tempFile.Name())
	defer tempFile.Close()
	fmt.Fprintf(tempFile, "[DEFAULT]\nuser=SomeUserWhoShouldntExist\n")
	_, _, err = UidFromConf(tempFile.Name())
	require.NotNil(t, err)
}

func TestHasSection(t *testing.T) {
	tempFile, err := ioutil.TempFile("", "INI")
	require.Nil(t, err)
	defer os.RemoveAll(tempFile.Name())
	tempFile.WriteString("[stuff]\ntruevalue=true\nfalsevalue=false\nintvalue=3\nset log_facility = LOG_LOCAL1\n")
	iniFile, err := LoadConfig(tempFile.Name())
	require.Nil(t, err)
	require.True(t, iniFile.HasSection("stuff"))
	require.False(t, iniFile.HasSection("otherstuff"))
}

func fakeHashPrefixAndSuffix() (filename string, err error) {
	var config_source []byte = []byte(
		"[swift-hash]\n" +
			"swift_hash_path_suffix = 983abc1de3ff4258\n")
	tempFile, err := ioutil.TempFile("", "swift.conf-")
	if err != nil {
		return "", err
	}
	ioutil.WriteFile(tempFile.Name(), config_source, 0600)
	configLocations = []string{tempFile.Name()}
	return tempFile.Name(), nil
}

func TestGetHashPrefixAndSuffix(t *testing.T) {
	swift_conf_name, err := fakeHashPrefixAndSuffix()
	assert.Nil(t, err)
	defer os.Remove(swift_conf_name)

	_, suffix, err := GetHashPrefixAndSuffix()
	assert.Nil(t, err, "Error getting hash path prefix or suffix")

	if suffix == "" {
		t.Error("Error prefix and suffix not being set")
	}
}

func TestReadResellerOptions(t *testing.T) {
	defaultRules := map[string][]string{"operator_roles": {"admin", "swiftoperator"},
		"service_roles": {}}
	var tests = []struct {
		s        string   // input
		prefixes []string // expected result
		options  map[string]map[string][]string
	}{
		{"[filter:keystoneauth]\n",
			[]string{"AUTH_"},
			map[string]map[string][]string{"AUTH_": defaultRules}},
		{"[filter:keystoneauth]\nreseller_prefix=AUTH\noperator_roles=admin, swiftoperator\n",
			[]string{"AUTH_"},
			map[string]map[string][]string{"AUTH_": defaultRules}},
		{"[filter:keystoneauth]\nreseller_prefix=\n",
			[]string{""},
			map[string]map[string][]string{"": defaultRules}},
		{"[filter:keystoneauth]\nreseller_prefix=\n''operator_roles=role1,role2",
			[]string{""},
			map[string]map[string][]string{"": {"operator_roles": {"role1", "role2"},
				"service_roles": {}}}},
		{"[filter:keystoneauth]\nreseller_prefix= '' , '' \n",
			[]string{""},
			map[string]map[string][]string{"": defaultRules}},
		{"[filter:keystoneauth]\nreseller_prefix=_,_\n",
			[]string{"_"},
			map[string]map[string][]string{"_": defaultRules}},
		{"[filter:keystoneauth]\nreseller_prefix=AUTH, PRE2, AUTH, PRE2\n",
			[]string{"AUTH_", "PRE2_"},
			map[string]map[string][]string{"AUTH_": defaultRules, "PRE2_": defaultRules}},
		{"[filter:keystoneauth]\nreseller_prefix=AUTH, PRE2, AUTH, PRE2\n",
			[]string{"AUTH_", "PRE2_"},
			map[string]map[string][]string{"AUTH_": defaultRules, "PRE2_": defaultRules}},
		{"[filter:keystoneauth]\nreseller_prefix= , , , \n",
			[]string{""},
			map[string]map[string][]string{"": defaultRules}},
		{"[filter:keystoneauth]\nreseller_prefix= PRE1, PRE2\n" +
			"PRE1_operator_roles= role1, role2\n" +
			"PRE1_service_roles= role3, role4\n" +
			"PRE2_operator_roles=role5\n" +
			"PRE2_service_roles=role6\n",
			[]string{"PRE1_", "PRE2_"},
			map[string]map[string][]string{"PRE1_": {"operator_roles": {"role1", "role2"},
				"service_roles": {"role3", "role4"}},
				"PRE2_": {"operator_roles": {"role5"},
					"service_roles": {"role6"}}}},
		{"[filter:keystoneauth]\nreseller_prefix= AUTH ,'', PRE2\n" +
			"operator_roles= role1, role2\n" +
			"service_roles= role3, role4\n" +
			"PRE2_operator_roles=role5\n" +
			"PRE2_service_roles=role6\n",
			[]string{"AUTH_", "", "PRE2_"},
			map[string]map[string][]string{"AUTH_": {"operator_roles": {"role1", "role2"},
				"service_roles": {"role3", "role4"}},
				"": {"operator_roles": {"role1", "role2"},
					"service_roles": {"role3", "role4"}},
				"PRE2_": {"operator_roles": {"role5"},
					"service_roles": {"role6"}}}},
		{"[filter:keystoneauth]\nreseller_prefix=AUTH ,, PRE2\n" +
			"''operator_roles=role1, role2\n" +
			"''service_roles=role3, role4\n" +
			"PRE2_operator_roles=role5\n" +
			"PRE2_service_roles=role6\n",
			[]string{"AUTH_", "PRE2_"},
			map[string]map[string][]string{"AUTH_": {"operator_roles": {"admin", "swiftoperator"},
				"service_roles": {}},
				"PRE2_": {"operator_roles": {"role5"},
					"service_roles": {"role6"}}}},
	}
	for _, tt := range tests {
		tempFile, err := ioutil.TempFile("", "INI")
		require.Nil(t, err)
		defer os.RemoveAll(tempFile.Name())
		tempFile.WriteString(tt.s)
		conf, err := LoadConfig(tempFile.Name())
		assert.Nil(t, err)
		confSec := conf.GetSection("filter:keystoneauth")
		prefixes, options := ReadResellerOptions(confSec, defaultRules)
		if !reflect.DeepEqual(prefixes, tt.prefixes) {
			t.Errorf("ReadResellerOptions: expected prefixes %v, actual prefixes %v", tt.prefixes, prefixes)
		}
		if !reflect.DeepEqual(options, tt.options) {
			t.Errorf("ReadResellerOptions: expected options %v, actual options %v", tt.options, options)
		}
	}
}
