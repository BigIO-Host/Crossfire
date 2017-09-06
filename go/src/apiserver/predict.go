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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"io/ioutil"
	"net/http"
)

type predictRequest struct {
	States    []event  `json:"states"`
	Window    int64    `json:"window"`
	Goals     []string `json:"goals"`
	CtxLength int64    `json:"ctxlength"` //hidden in doc
}

type predictResult struct {
	Goal        string     `json:"goal"`
	Probability float64    `json:"probability"`
	Error       bigioError `json:"item-status"`
}

// this matches response json result
type predictResponse struct {
	Results []predictResult `json:"results"`
	Error   bigioError      `json:"status"`
}

func classifyHandlerCommon(w http.ResponseWriter, r *http.Request, body []byte) []byte {
	c := context.Background()
	// get projectId from URL
	projectID := mux.Vars(r)["projectId"]
	sessionID := mux.Vars(r)["sessionId"]
	bigioDebugf(c, string(projectID))
	bigioDebugf(c, string(sessionID))

	var req predictRequest
	err := json.Unmarshal(body, &req)
	if err != nil {
		http.Error(w, "json parse error\n", 400)
		return []byte("")
	}

	var windowDays int64
	if req.Window == 0 {
		windowDays = 3
	} else {
		windowDays = req.Window
	}
	startDate, endDate := getDatestamps(windowDays)

	if len(req.Goals) == 0 || len(req.Goals) > 5 {
		http.Error(w, "Min 1 goal, Max 5 goals in one API call\n", 400)
		return []byte("")
	}

	// build supplemental historical state in "type#name" style from request event array
	reqStates, err := validateEachEventType(req.States)
	if err != nil {
		http.Error(w, fmt.Sprintf("Json field : %v, the event(s) after this are not processed", err.Error()), 400)
	}

	// retrieve context+atribute state from session_store
	var states []string
	CtxLength := req.CtxLength
	if len(sessionID) > 0 && CtxLength != -1 {
		if CtxLength == 0 {
			CtxLength = 10
		}
		states = sessionStore.mergeStates(projectID, sessionID, CtxLength, reqStates)
		bigioDebugf(c, "events with session store: %v", states)
	} else {
		states = reqStates
		bigioDebugf(c, "history events without session store: %v", states)
	}

	var resResults = make([]predictResult, 0)

	for _, goalName := range req.Goals {
		bigioDebugf(c, "Query Goal=%s", goalName)
		// FIXME: workaround for feedback removal
		// truePosterior, _, status, errorMsg, predictors, priorT, priorF, eventTLikes, eventFLikes, evtLength := mutantClassify(projectID, goalNameTag, states, startDate, endDate)
		truePosterior, status, errorMsg := mutantClassify(projectID, goalName, states, startDate, endDate)

		//append enture struct instead of pointer reference
		resResults = append(resResults, predictResult{
			Goal:        goalName,
			Probability: truePosterior,
			Error:       bigioError{Code: status, Message: errorMsg}})

	}

	res := &predictResponse{
		Results: resResults,
		Error:   bigioError{Code: 0, Message: ""}}
	returnBody, _ := json.Marshal(res)

	return returnBody
}

func classifyHandlerPOST(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "unsupported HTTP verb", 400)
		return
	}

	c := context.Background()
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576)) //max json input 1MB
	if err != nil {
		http.Error(w, "json payload too large, max 1 MByte\n", 400) //413
		return
	}
	bigioDebugf(c, "%s", body)

	returnBody := classifyHandlerCommon(w, r, body)
	w.Write(returnBody)
}

func classifyHandlerGET(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "unsupported HTTP verb", 400)
		return
	}

	c := context.Background()

	//get it from Querystring "data=", base64 encoded
	base64Str := r.URL.Query().Get("data")
	body, err := base64.StdEncoding.DecodeString(base64Str)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	bigioDebugf(c, "%s", body)

	returnBody := classifyHandlerCommon(w, r, body)
	w.Write(returnBody)
}
