// Copyright 2021-2022 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package connect

import (
	"strings"
)

// extractProtoPath returns the trailing portion of the URL's path,
// corresponding to the Protobuf package, service, and method. It always starts
// with a slash. Within connect, we use this as (1) Spec.Procedure and (2) the
// path when mounting handlers on muxes.
func extractProtoPath(url string) string {
	segments := strings.Split(url, "/")
	switch segmentsLen := len(segments); {
	case segmentsLen > 1:
		pkg, method := segments[segmentsLen-2], segments[segmentsLen-1]
		return "/" + pkg + "/" + method
	case segmentsLen == 1:
		pkg := segments[0]
		return "/" + pkg
	default:
		return "/"
	}
}
