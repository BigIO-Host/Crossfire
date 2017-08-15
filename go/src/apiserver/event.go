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
	"fmt"
	_ "net/http"
	"regexp"
	"strings"
)

type bigioError struct {
	Code    int64  `json:"code"`
	Message string `json:"message"`
}

// event object used in API json
type event struct {
	Name  string  `json:"name"`
	Type  string  `json:"type"`
	Value float64 `json:"value"` //only apply to goal
	Label string  `json:"label"`
	Hint  string  `json:"hint"` //only apply to goal
}

// event object for backend pubsub messsage
type pubSubEvent struct {
	ProjectID  string  `json:"projectid"`
	ID         string  `json:"id"`
	EventName  string  `json:"event"`
	EventType  string  `json:"type"`
	EventValue float64 `json:"value"`
	Label      string  `json:"label"`
	Hint       string  `json:"hint"`
	Timestamp  int64   `json:"timestamp"`
	SsnConf    string  `json:"sessionConf"`
}

type invalidEventTypeError struct {
}

func (e invalidEventTypeError) Error() string {
	return fmt.Sprintf("invalid event type found")
}

func validateEachEventType(rawReqStates []event) ([]string, error) {
	validEventTypeReg, _ := regexp.Compile(validEventTypeRegx)
	var reqStates = make([]string, len(rawReqStates))

	for i, state := range rawReqStates {
		if !validEventTypeReg.MatchString(state.Type) {
			return reqStates, invalidEventTypeError{}
		}

		reqStates[i] = fmt.Sprintf("%s#%s", strings.ToUpper(state.Type), state.Name) //change to prefix model
	}
	return reqStates, nil
}
