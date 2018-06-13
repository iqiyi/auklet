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

package common

// Server Types
const (
	AccountServer   = "Account"
	ContainerServer = "Container"
	ObjectServer    = "Object"
)

// Replcator relative
const (
	ONE_WEEK                = 604800
	HASH_FILE               = "hashes.pkl"
	HASH_INVALIDATIONS_FILE = "hashes.invalid"
)

// Backend header names
const (
	XBackendPolicyIndex        = "X-Backend-Storage-Policy-Index"
	XBackendObjLength          = "X-Backend-Obj-Content-Length"
	XBackendEtagIsAt           = "X-Backend-Etag-Is-At"
	XBackendTimestamp          = "X-Backend-Timestamp"
	XBackendReplicationHeaders = "X-Backend-Replication-Headers"
	XCopyFrom                  = "X-Copy-From"
	XDeleteAtContainer         = "X-Delete-At-Container"
	XDeleteAtPartition         = "X-Delete-At-Partition"
	XDeleteAtHost              = "X-Delete-At-Host"
	XDeleteAtDevice            = "X-Delete-At-Device"
	XAccountHost               = "X-Account-Host"
	XAccountDevice             = "X-Account-Device"
	XAccountPartition          = "X-Account-Partition"

	XContainerHost      = "X-Container-Host"
	XContainerDevice    = "X-Container-Device"
	XContainerPartition = "X-Container-Partition"
	XStaticLargeObject  = "X-Static-Large-Object"
	XObjectManifest     = "X-Object-Manifest"
	XIfDeleteAt         = "X-If-Delete-At"
	XForceAcquire       = "X-Force-Acquire"
	XDiskUsage          = "X-Disk-Usage"
)

// Client header names
const (
	XStoragePolicy = "X-Storage-Policy"
	XTimestamp     = "X-Timestamp"
	XDeleteAt      = "X-Delete-At"
	XDeleteAfter   = "X-Delete-After"
	XTransId       = "X-Trans-Id"
	XContentType   = "X-Content-Type"
	XSize          = "X-Size"
	XEtag          = "X-ETag"

	IfMatch           = "If-Match"
	IfNoneMatch       = "If-None-Match"
	IfUnmodifiedSince = "If-Unmodified-Since"
	IfModifiedSince   = "If-Modified-Since"

	LastModified = "Last-Modified"
)

// Standard header names
const (
	HContentType        = "Content-Type"
	HContentLength      = "Content-Length"
	HExpect             = "Expect"
	HEtag               = "ETag"
	HContentEncoding    = "Content-Encoding"
	HContentDisposition = "Content-Disposition"
	HAcceptRanges       = "Accept-Ranges"
	HRange              = "Range"
	HContentRange       = "Content-Range"
	HReferer            = "Referer"
	HUserAgent          = "User-Agent"

	XContentTypeOptions = "X-Content-Type-Options"
)

// Standard header value
const (
	VOctetStream = "application/octet-stream"
	VTextPlain   = "text/plain"
	V100Continue = "100-Continue"
	VTextHtml    = "text/html; charset=UTF-8"
	VChunked     = "chunked"
	VBytes       = "bytes"
	VNoSniff     = "nosniff"
)

// Non standard Methods
const (
	REPLICATE = "REPLICATE"
	SYNC      = "SYNC"
)

// Meta header prefix in lower case
const (
	ObjectSysMetaPrefix          = "x-object-sysmeta-"
	ObjectTransientSysMetaPrefix = "x-object-transient-sysmeta-"
)

// Non standard status code
const (
	StatusClientClosedRequest = 499
	StatusRateLimited         = 488
)

// Non  standard status text
var statusText = map[int]string{
	StatusClientClosedRequest: "Client Disconnect",
	StatusRateLimited:         "Rate Limited",
}

func StatusText(code int) string {
	// empty string returned if the code is unknown
	return statusText[code]
}
