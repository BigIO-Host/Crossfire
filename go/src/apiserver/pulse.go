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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub"
	"context"

	s "strings"
)

type pulseRequest struct {
	Events    []event `json:"events"`
	Timestamp string  `json:"timestamp"`
	SsnTag    string  `json:"sessionTag"` // FIXME: test purpose
}

const maxEventsPerCall = 25
const maxSsnFieldLength = 60
const maxTxtFieldLength = 40

var validEventTypeReg, _ = regexp.Compile(validEventTypeRegx)

// fire an event into the pubsub queue
func fireEvent(evt pubSubEvent) error {
	c := context.Background()

	// push into temp. session store
	sessionStore.append(&evt)

	// sending event into pubsub
	body, _ := json.Marshal(evt)
	//log.Debugf(c, "json ok")
	msg := &pubsub.Message{
		Data: body,
		Attributes: map[string]string{
			"timestamp": strconv.FormatInt(evt.Timestamp, 10),
		},
	}
	//log.Debugf(c, "msg ok")
	if _, err := topic.Publish(c, msg).Get(c); err != nil {
		bigioErrorf(c, "Pubsub Error: %v", err)
		return fmt.Errorf("Could not publish message: %v", err)
	}
	bigioDebugf(c, "fired event: %v", evt)

	return nil
}

func pulseHandlerCommon(w http.ResponseWriter, r *http.Request, body []byte) int64 {
	//Common routine
	projectID := mux.Vars(r)["projectId"]
	sessionID := mux.Vars(r)["sessionId"]

	var evtreq pulseRequest
	err := json.Unmarshal(body, &evtreq)
	if err != nil {
		http.Error(w, "Input json parse error, check syntax or data type", 400)
		return 1
	}

	var timestamp int64
	if len(evtreq.Timestamp) > 0 {
		time, err := time.Parse(time.RFC3339, evtreq.Timestamp)
		if err != nil {
			http.Error(w, "timestamp parse error. RFC3339 YYYY-MM-DDTHH:MI:SS.SSSZ", 400)
			return 1
		}

		timestamp = time.UnixNano() / 1000000

	} else {
		timestamp = time.Now().UnixNano() / 1000000
	}

	// if more than 25 event(s) in events array
	if len(evtreq.Events) > maxEventsPerCall {
		http.Error(w, "Max 25 evetns per api call, check num of events in json", 400)
		return 1
	}

	// id field needs to less than 60 characters
	if len(sessionID) > maxSsnFieldLength {
		http.Error(w, "session id can not larger than 60 bytes", 400)
		return 1
	}

	var evt pubSubEvent
	evt.ProjectID = projectID //this is from URL wildcard pattern
	evt.ID = sessionID
	evt.Timestamp = timestamp

	evt.SsnConf = r.Context().Value("ssnConf").(string)

	/* multiple event */
	//for each event in events
	for _, event := range evtreq.Events {
		// event name can not longer than 40 characters
		if len(event.Name) > maxTxtFieldLength {
			http.Error(w, "Json field : event can not be larger than 40 bytes, the event(s) after this are not processed", 400)
			return 1
		}

		if len(event.Type) == 0 {
			event.Type = "event"
		}

		if len(event.Label) > maxTxtFieldLength {
			http.Error(w, "Json field : label can not be larger than 40 bytes, the event(s) after this are not processed", 400)
			return 1
		}

		if len(event.Hint) > maxTxtFieldLength {
			http.Error(w, "Json field : hint can not be larger than 40 bytes, the event(s) after this are not processed", 400)
			return 1
		}

		if !validEventTypeReg.MatchString(event.Type) {
			http.Error(w, "Json field : invalid event type, the event(s) after this are not processed", 400)
			return 1
		}

		if event.Value != 0 && event.Type != "goal" { //FIXME: this is not bullet proof check, user can still send value=0.
			http.Error(w, "Json field : Only goal event can have value property, the event(s) after this are not processed", 400)
			return 1
		}

		evt.EventType = s.ToUpper(event.Type)
		evt.EventName = event.Name //prefix

		evt.EventValue = event.Value //only apply to Goal
		evt.Label = event.Label
		evt.Hint = event.Hint //only apply to Goal

		err = fireEvent(evt)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return 1
		}
	}
	return 0
}

func pulseHandlerPOST(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "unsupported HTTP verb", 400)
		return
	}

	c := context.Background()

	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576)) //max json input 1MB
	if err != nil {
		http.Error(w, "Json payload too large, max 1 MByte\n", 400) //413
	}

	bigioDebugf(c, string(body))

	returnCode := pulseHandlerCommon(w, r, body)
	fmt.Fprint(w, returnCode)
}

func pulseHandlerGET(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "unsupported HTTP verb", 400)
		return
	}

	c := context.Background()

	base64Str := r.URL.Query().Get("data")
	body, err := base64.StdEncoding.DecodeString(base64Str)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	bigioDebugf(c, "%s", body)

	returnCode := pulseHandlerCommon(w, r, body)

	fmt.Fprint(w, returnCode)
}
