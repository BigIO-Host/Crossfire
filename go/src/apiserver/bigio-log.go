// Copyright 2017 BigIO.host. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"log"
)

func bigioDebugf(ctx context.Context, format string, args ...interface{}) {
	if enableDebug {
		//log.Debugf(ctx, format, args...)
		log.Printf("DEBUG: "+format, args...)
		//lg.Log(logging.Entry{Payload: fmt.Sprintf(format, args...), Severity: logging.Debug})
		//logger.Log(logging.Entry{Payload: "OK-XDDDDD", Severity: logging.Debug})
		//lg.Flush() // with log flush is very very slow!! Don't do it!
	}
}

func bigioInfof(ctx context.Context, format string, args ...interface{}) {
	if enableDebug {
		//log.Infof(ctx, format, args...)
		log.Printf("INFO: "+format, args...)
	}
}

func bigioWarningf(ctx context.Context, format string, args ...interface{}) {
	if enableDebug {
		//log.Warningf(ctx, format, args...)
		log.Printf("WARNING: "+format, args...)
	}
}

func bigioErrorf(ctx context.Context, format string, args ...interface{}) {
	if enableDebug {
		//log.Errorf(ctx, format, args...)
		log.Printf("ERROR: "+format, args...)
	}
}

func bigioCriticalf(ctx context.Context, format string, args ...interface{}) {
	if enableDebug {
		//log.Criticalf(ctx, format, args...)
		log.Printf("CRITICAL: "+format, args...)
	}
}
