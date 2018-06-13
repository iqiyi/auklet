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

package common

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMultiWriter(t *testing.T) {
	w := &bytes.Buffer{}
	mw := NewMultiWriter(w, "text/plain", 100)

	boundary := mw.Boundary()

	p, _ := mw.CreatePart(0, 3)
	p.Write([]byte("HI"))

	p, _ = mw.CreatePart(4, 9)
	p.Write([]byte("THERE"))

	mw.Close()
	shouldBe := "--" + boundary + "\r\nContent-Type: text/plain\r\nContent-Range: bytes 0-2/100\r\n\r\nHI\r\n--" +
		boundary + "\r\nContent-Type: text/plain\r\nContent-Range: bytes 4-8/100\r\n\r\nTHERE\r\n--" + boundary + "--"
	assert.Equal(t, shouldBe, string(w.Bytes()))
}

func TestMultiWriterClosedPart(t *testing.T) {
	w := &bytes.Buffer{}
	mw := NewMultiWriter(w, "text/plain", 100)
	p1, _ := mw.CreatePart(0, 10)
	mw.CreatePart(10, 20)
	_, err := p1.Write([]byte("HI"))
	assert.NotNil(t, err)
}

type FailWriter struct {
	n int
}

func (f *FailWriter) Write(d []byte) (n int, err error) {
	if f.n > 0 {
		return 0, errors.New("SOME ERROR")
	}
	f.n += 1
	return len(d), nil
}

func TestMultiWriterFails(t *testing.T) {
	mw := NewMultiWriter(&FailWriter{0}, "text/plain", 100)

	p, _ := mw.CreatePart(0, 10)
	_, err := p.Write([]byte("HI"))
	assert.NotNil(t, err)
	assert.NotNil(t, mw.Close())
	_, err = mw.CreatePart(10, 20)
	assert.NotNil(t, err)
}
