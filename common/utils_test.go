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
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRange(t *testing.T) {
	//Setting up individual test data
	tests := []struct {
		fileSize    int64
		rangeHeader string
		exError     string
		exRanges    []HttpRange
	}{
		{12345, "bytes=12346-123457", "Unsatisfiable range", nil},
		{12345, "bytes=12346-", "Unsatisfiable range", nil},
		{12345, " ", "", nil},
		{12345, "nonbytes=1-2", "", nil},
		{12345, "bytes=", "", nil},
		{12345, "bytes=-", "", nil},
		{12345, "bytes=-cv", "", nil},
		{12345, "bytes=cv-", "", nil},
		{12345, "bytes=-12346", "", []HttpRange{{0, 12345}}},
		{12345, "bytes=-12345", "", []HttpRange{{0, 12345}}},
		{12345, "bytes=-12344", "", []HttpRange{{1, 12345}}},
		{12345, "bytes=12344-", "", []HttpRange{{12344, 12345}}},
		{12345, "bytes=12344-cv", "", nil},
		{12345, "bytes=13-12", "", nil},
		{12345, "bytes=cv-12345", "", nil},
		{12345, "bytes=12342-12343", "", []HttpRange{{12342, 12344}}},
		{12345, "bytes=12342-12344", "", []HttpRange{{12342, 12345}}},
		{12345, "bytes=12342-12345", "", []HttpRange{{12342, 12345}}},
		{12345, "bytes=0-1,2-3", "", []HttpRange{{0, 2}, {2, 4}}},
		{12345, "bytes=0-1,x-x", "", nil},
		{12345, "bytes=0-1,x-x,2-3", "", nil},
		{12345, "bytes=0-1,x-", "", nil},
		{12345, "bytes=0-1,2-3,4-5", "", []HttpRange{{0, 2}, {2, 4}, {4, 6}}},
		{10000, "bytes=0-99,200-299,10000-10099", "", []HttpRange{{0, 100}, {200, 300}}},
		{10000, "bytes=-0", "Unsatisfiable range", nil},
		{10000, "bytes=-0", "Unsatisfiable range", nil},
		{10000, "bytes=0-1,-0", "", []HttpRange{{0, 2}}},
		{0, "bytes=-0", "Unsatisfiable range", nil},
		{0, "bytes=0-", "Unsatisfiable range", nil},
		{0, "bytes=0-1", "Unsatisfiable range", nil},
		{10000, "bytes=0-1,1-2,2-3,3-4,4-5,5-6,6-7,7-8,8-9,9-10,10-11,11-12,12-13,13-14,14-15,15-16,16-17," +
			"17-18,18-19,19-20,20-21,21-22,22-23,23-24,24-25,25-26,26-27,27-28,28-29,29-30,30-31,31-32," +
			"32-33,33-34,34-35,35-36,36-37,37-38,38-39,39-40,40-41,41-42,42-43,43-44,44-45,45-46,46-47," +
			"47-48,48-49,49-50,50-51,51-52,52-53,53-54,54-55,55-56,56-57,57-58,58-59,59-60,60-61,61-62," +
			"62-63,63-64,64-65,65-66,66-67,67-68,68-69,69-70,70-71,71-72,72-73,73-74,74-75,75-76,76-77," +
			"77-78,78-79,79-80,80-81,81-82,82-83,83-84,84-85,85-86,86-87,87-88,88-89,89-90,90-91,91-92," +
			"92-93,93-94,94-95,95-96,96-97,97-98,98-99,99-100,100-101", "Too many ranges", nil},
	}

	//Run tests with data from above
	for _, test := range tests {
		result, err := ParseRange(test.rangeHeader, test.fileSize)
		if test.rangeHeader == " " {
			require.Nil(t, result, test.rangeHeader)
			require.Nil(t, err, test.rangeHeader)
			continue
		}
		if test.exError == "" {
			require.Nil(t, err, test.rangeHeader)
		} else {
			require.NotNil(t, err, test.rangeHeader)
			require.Equal(t, test.exError, err.Error(), test.rangeHeader)
		}
		require.Equal(t, len(result), len(test.exRanges), test.rangeHeader)
		for i := range result {
			require.Equal(t, test.exRanges[i], result[i], test.rangeHeader)
		}
	}
}

func TestParseDate(t *testing.T) {
	//Setup tests with individual data
	tests := []string{
		"Mon, 02 Jan 2006 15:04:05 MST",
		"Mon, 02 Jan 2006 15:04:05 -0700",
		"Mon Jan 02 15:04:05 2006",
		"Monday, 02-Jan-06 15:04:05 MST",
		"1136214245",
		"1136214245.1234",
		"2006-01-02 15:04:05",
	}

	//Run Tests from above
	for _, timestamp := range tests {
		timeResult, err := ParseDate(timestamp)
		if err == nil {
			assert.Equal(t, timeResult.Day(), 2)
			assert.Equal(t, timeResult.Month(), time.Month(1))
			assert.Equal(t, timeResult.Year(), 2006)
			assert.Equal(t, timeResult.Hour(), 15)
			assert.Equal(t, timeResult.Minute(), 4)
			assert.Equal(t, timeResult.Second(), 5)
		} else {
			assert.Equal(t, err.Error(), "invalid time")
		}
	}

}

func TestStandardizeTimestamp(t *testing.T) {
	//Setup tests with individual data
	tests := []struct {
		timestamp      string
		expectedResult string
	}{
		{"12345.12345", "0000012345.12345"},
		{"12345.1234", "0000012345.12340"},
		{"12345.1234_123455", "0000012345.12340_0000000000123455"},
		{"12345.12343_12345a", "0000012345.12343_000000000012345a"},
	}

	//Run Tests from above
	for _, test := range tests {
		result, _ := StandardizeTimestamp(test.timestamp)
		assert.Equal(t, test.expectedResult, result)
	}

}

func TestStandardizeTimestamp_invalidTimestamp(t *testing.T) {
	//Setup test data
	tests := []struct {
		timestamp string
		errorMsg  string
	}{
		{"invalidTimestamp", "Could not parse float from 'invalidTimestamp'."},
		{"1234.1234_invalidOffset", "Could not parse int from 'invalidOffset'."},
	}
	for _, test := range tests {
		_, err := StandardizeTimestamp(test.timestamp)
		assert.Equal(t, err.Error(), test.errorMsg)
	}
}

func TestGetEpochFromTimestamp(t *testing.T) {
	//Setup tests with individual data
	tests := []struct {
		timestamp      string
		expectedResult string
	}{
		{"12345.12345", "0000012345.12345"},
		{"12345.1234", "0000012345.12340"},
		{"12345.1234_123455", "0000012345.12340"},
		{"12345.12343_12345a", "0000012345.12343"},
	}

	//Run Tests from above
	for _, test := range tests {
		result, _ := GetEpochFromTimestamp(test.timestamp)
		assert.Equal(t, test.expectedResult, result)
	}
}

func TestGetEpochFromTimestamp_invalidTimestamp(t *testing.T) {
	_, err := GetEpochFromTimestamp("invalidTimestamp")
	assert.Equal(t, err.Error(), "Could not parse float from 'invalidTimestamp'.")
}

func TestLooksTrue(t *testing.T) {
	tests := []string{
		"true ", "true", "t", "yes", "y", "1", "on",
	}
	for _, test := range tests {
		assert.True(t, LooksTrue(test))
	}
}

func TestUrlencode(t *testing.T) {
	assert.True(t, Urlencode("HELLO%2FTHERE") == "HELLO%252FTHERE")
	assert.True(t, Urlencode("HELLOTHERE") == "HELLOTHERE")
	assert.True(t, Urlencode("HELLO THERE, YOU TWO//\x00\xFF") == "HELLO%20THERE%2C%20YOU%20TWO//%00%FF")
	assert.True(t, Urlencode("鐋댋") == "%E9%90%8B%EB%8C%8B")
}

func TestCopy(t *testing.T) {
	src := bytes.NewBuffer([]byte("WELL HELLO THERE"))
	dst1 := &bytes.Buffer{}
	dst2 := &bytes.Buffer{}
	Copy(src, dst1, dst2)
	assert.Equal(t, []byte("WELL HELLO THERE"), dst1.Bytes())
	assert.Equal(t, []byte("WELL HELLO THERE"), dst2.Bytes())
}

func TestCopyN(t *testing.T) {
	src := bytes.NewBuffer([]byte("WELL HELLO THERE"))
	dst1 := &bytes.Buffer{}
	dst2 := &bytes.Buffer{}
	CopyN(src, 10, dst1, dst2)
	assert.Equal(t, []byte("WELL HELLO"), dst1.Bytes())
	assert.Equal(t, []byte("WELL HELLO"), dst2.Bytes())
}

func TestParseProxyPath(t *testing.T) {
	tests := [][]string{
		{"", "vrs", ""},
		{"", "account", ""},
		{"/", "vrs", ""},
		{"/", "object", ""},
		{"/v/a/c/o/a/b/c/d", "vrs", "v"},
		{"/v/a/c/o/a/b/c/d", "account", "a"},
		{"/v/a/c/o/a/b/c/d", "container", "c"},
		{"/v/a/c/o/a/b/c/d", "object", "o/a/b/c/d"},
		{"/v/a/c/o/a/b/c///d", "object", "o/a/b/c///d"},
		{"/v/a/c//o/b/c///d", "object", "/o/b/c///d"},
		{"/v", "vrs", "v"},
		{"/v/", "vrs", "v"},
		{"/v/a", "account", "a"},
		{"/v/a/", "account", "a"},
		{"/v/a/", "container", ""},
	}
	for i := 0; i < len(tests); i++ {
		tSlice := tests[i]
		res, err := ParseProxyPath(tSlice[0])
		assert.Equal(t, res[tSlice[1]], tSlice[2])
		assert.Equal(t, err, nil)
	}
	res, err := ParseProxyPath("v/a")
	assert.Equal(t, len(res), 0)
	assert.NotEqual(t, err, nil)
	res, err = ParseProxyPath("//")
	assert.Equal(t, len(res), 0)
	assert.NotEqual(t, err, nil)
	res, err = ParseProxyPath("/v//c")
	assert.Equal(t, len(res), 0)
	assert.NotEqual(t, err, nil)
}

func TestCheckNameFormat(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	s := "asdf"
	target := "ignored"

	checked, err := CheckNameFormat(req, s, target)
	require.Nil(t, err)
	require.Equal(t, checked, s)
}

func TestNoNameSlashes(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	s := "as/df"
	target := "ignored"

	checked, err := CheckNameFormat(req, s, target)
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "ignored name cannot contain slashes")
	require.Equal(t, checked, "")
}

func TestNoEmptyName(t *testing.T) {
	req, err := http.NewRequest("PUT", "/v1/a/c/o", nil)
	s := ""
	target := "ignored"

	checked, err := CheckNameFormat(req, s, target)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "ignored name cannot be empty")
	require.Equal(t, checked, "")
}

func TestParseContentTypeForSlo(t *testing.T) {
	ct, size, err := ParseContentTypeForSlo("text/html", int64(12))
	require.Equal(t, ct, "text/html")
	require.Equal(t, size, int64(12))
	require.Nil(t, err)

	ct, size, err = ParseContentTypeForSlo("text/html;swift_bytes=36", int64(12))
	require.Equal(t, ct, "text/html")
	require.Equal(t, size, int64(36))
	require.False(t, strings.Contains(ct, ";"))
	require.Nil(t, err)

	ct, size, err = ParseContentTypeForSlo("text/html;swift_bytes=moo", int64(12))
	require.Equal(t, ct, "")
	require.NotNil(t, err)
}

func TestSliceFromCSV(t *testing.T) {
	var tests = []struct {
		s        string   // input
		expected []string // expected result
	}{
		{"", []string{}},
		{"fdfd", []string{"fdfd"}},
		{"fd,fd", []string{"fd", "fd"}},
		{"fd,   fd", []string{"fd", "fd"}},
		{" fd  , fd ", []string{"fd", "fd"}},
		{"fd fd", []string{"fd fd"}},
		{"fd,fd, fdf", []string{"fd", "fd", "fdf"}},
		{"fd,,fd", []string{"fd", "fd"}},
	}

	for _, tt := range tests {
		actual := SliceFromCSV(tt.s)
		if !reflect.DeepEqual(actual, tt.expected) {
			t.Errorf("SliceFromCSV(%v): expected %v, actual %v", tt.s, tt.expected, actual)
		}
	}
}
