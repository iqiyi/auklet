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

package fs

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsMount(t *testing.T) {
	isMount, err := IsMount("/dev")
	assert.Nil(t, err)
	assert.True(t, isMount)
	isMount, err = IsMount(".")
	assert.Nil(t, err)
	assert.False(t, isMount)
	isMount, err = IsMount("/slartibartfast")
	assert.NotNil(t, err)
}

func TestIsNotDir(t *testing.T) {
	tempFile, _ := ioutil.TempFile("", "INI")
	defer os.RemoveAll(tempFile.Name())
	_, err := ioutil.ReadDir(tempFile.Name())
	assert.True(t, IsNotDir(err))
	_, err = ioutil.ReadDir("/aseagullstolemysailorhat")
	assert.True(t, IsNotDir(err))
}

func TestReadDirNames(t *testing.T) {
	tempDir, _ := ioutil.TempDir("", "RDN")
	defer os.RemoveAll(tempDir)
	ioutil.WriteFile(tempDir+"/Z", []byte{}, 0666)
	ioutil.WriteFile(tempDir+"/X", []byte{}, 0666)
	ioutil.WriteFile(tempDir+"/Y", []byte{}, 0666)
	fileNames, err := ReadDirNames(tempDir)
	assert.Nil(t, err)
	assert.Equal(t, fileNames, []string{"X", "Y", "Z"})
}

func TestLockPath(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "")
	defer os.RemoveAll(tempDir)
	require.Nil(t, err)
	c := make(chan bool)
	ended := make(chan struct{})
	defer close(ended)
	go func() {
		f, err := LockPath(tempDir, time.Millisecond)
		c <- true
		require.Nil(t, err)
		require.NotNil(t, f)
		defer f.Close()
		select {
		case <-time.After(time.Second):
		case <-ended:
		}
	}()
	<-c
	f, err := LockPath(tempDir, time.Millisecond)
	require.Nil(t, f)
	require.NotNil(t, err)
}
