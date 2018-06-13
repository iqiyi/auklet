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

// TODO: Some of this code was pulled from the go stdlib and modified. figure out how to attribute this.
// https://wiki.openstack.org/wiki/LegalIssuesFAQ#Incorporating_BSD.2FMIT_Licensed_Code

package common

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

// MultiWriter is a streaming multipart writer, similar to the standar
// d library's multipart.Writer, but compatible with Swift's output and
// with an API geared toward our common use, multi-range responses.
type MultiWriter struct {
	w              io.Writer
	boundary       string
	lastpart       *part
	contentType    string
	contentLength  int64
	lengthEstimate int64
}

// NewMultiWriter instantiates a new MultiWriter.
func NewMultiWriter(
	w io.Writer, contentType string, contentLength int64) *MultiWriter {
	var buf [32]byte
	_, err := io.ReadFull(rand.Reader, buf[:])
	if err != nil {
		panic(err)
	}
	return &MultiWriter{
		w:              w,
		boundary:       fmt.Sprintf("%064x", buf[:]),
		contentType:    contentType,
		contentLength:  contentLength,
		lengthEstimate: int64(68), // length of --boundary--
	}
}

// Expect adds an expected part to the Content-Length estimate for
// the multipart body.
func (w *MultiWriter) Expect(start, end int64) {
	w.lengthEstimate += int64(len(fmt.Sprintf(
		"--%s\r\nContent-Type: %s\r\nContent-Range: bytes %d-%d/%d\r\n\r\n",
		w.boundary, w.contentType, start, end-1, w.contentLength)))
	w.lengthEstimate += (end - start) + 2 // part data and trailing \r\n
}

// ContentLength returns the expected Content-Length of the
// multipart body.
func (w *MultiWriter) ContentLength() int64 {
	return w.lengthEstimate
}

// Boundary returns the MultiWriter's boundary string.
func (w *MultiWriter) Boundary() string {
	return w.boundary
}

// CreatePart begins a new part in the multi-part response, with
// the given content ranges.
func (w *MultiWriter) CreatePart(start, end int64) (io.Writer, error) {
	if w.lastpart != nil {
		if err := w.lastpart.close(); err != nil {
			return nil, err
		}
	}
	b := &bytes.Buffer{}
	if w.lastpart != nil {
		fmt.Fprintf(b, "\r\n")
	}
	fmt.Fprintf(b,
		"--%s\r\nContent-Type: %s\r\nContent-Range: bytes %d-%d/%d\r\n\r\n",
		w.boundary, w.contentType, start, end-1, w.contentLength)
	_, err := io.Copy(w.w, b)
	if err != nil {
		return nil, err
	}
	p := &part{
		mw: w,
	}
	w.lastpart = p
	return p, nil
}

// Close finalizes the output of the MultiWriter.
func (w *MultiWriter) Close() error {
	if w.lastpart != nil {
		if err := w.lastpart.close(); err != nil {
			return err
		}
		w.lastpart = nil
	}
	_, err := fmt.Fprintf(w.w, "\r\n--%s--", w.boundary)
	return err
}

type part struct {
	mw     *MultiWriter
	closed bool
	we     error
}

func (p *part) close() error {
	p.closed = true
	return p.we
}

func (p *part) Write(d []byte) (n int, err error) {
	if p.closed {
		return 0, errors.New("multipart: can't write to finished part")
	}
	n, err = p.mw.w.Write(d)
	if err != nil {
		p.we = err
	}
	return
}
