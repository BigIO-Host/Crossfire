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
	_ "log"
	"net/http"
	_ "regexp"
	_ "strings"
	"sync"
	"time"
)

type action struct {
	Name string  `json:"name"`
	Cost float64 `json:"cost"`
}

type actionRequest struct {
	States           []event  `json:"states"`
	Window           int64    `json:"window"`
	Goal             string   `json:"goal"`
	Actions          []action `json:"actions"`
	Reward           float64  `json:"reward"`
	AnchorID         string   `json:"anchorid"`
	GainOnly         bool     `json:"gainonly"`
	Explore          string   `json:"explore"` // FIXME: hidden feature in API DOC
	MuteActionRecord bool     `json:"mute"`
	CtxLength        int64    `json:"ctxlength"`
	Label            string   `json:"label"` // FIXME: hidden feature: new Tag to match goal label and event label
	Detail           bool     `json:"detail"`
}

type actionResult struct {
	Goal        string     `json:"goal"`
	Action      string     `json:"action"`
	Probability float64    `json:"baseline-prob"`
	ActTrue     float64    `json:"action-prob"` // FIXME: WHAT?
	Lift        float64    `json:"prob-lift-pct"`
	Gain        float64    `json:"expected-gain"`
	Error       bigioError `json:"item-status"`
}

// this matches response json result
type actionResponse struct {
	Results  []actionResult `json:"results"`
	Goal     string         `json:"goal"`
	Decision string         `json:"decision"` // FIXME: missing in API DOC
	Mode     string         `json:"mode"`
	Error    bigioError     `json:"status"`
}

func actionHandlerCommon(w http.ResponseWriter, r *http.Request, body []byte) []byte {
	c := context.Background()
	// get projectId from URL
	projectID := mux.Vars(r)["projectId"]
	sessionID := mux.Vars(r)["sessionId"]
	bigioDebugf(c, string(projectID))
	bigioDebugf(c, string(sessionID))

	var req actionRequest
	err := json.Unmarshal(body, &req)
	if err != nil {
		bigioDebugf(c, string(body))
		http.Error(w, "json parse error\n", 400)
		return []byte("")
	}

	var windowDays int64
	if req.Window == 0 {
		windowDays = 7
	} else {
		windowDays = req.Window
	}
	startDate, endDate := getDatestamps(windowDays)

	var goal string
	goal = req.Goal //better validation, if no goal, we throw error
	if goal == "" {
		bigioDebugf(c, string(body))
		http.Error(w, "required field goal is missing or empty\n", 400)
		return []byte("")
	}
	bigioDebugf(c, "Query Goal=%s", goal)

	var reward float64
	reward = req.Reward //better validation, if no reward, we default it to 1.0
	bigioDebugf(c, "Reward=%f", reward)
	if reward == 0 {
		reward = 1.0
		bigioDebugf(c, "reset default Reward=%f", reward)
	}

	var gainonly bool
	gainonly = req.GainOnly
	/* actions and their costs are array */

	var label string
	label = req.Label

	bigioDebugf(c, "Actions len=%d", len(req.Actions))
	if len(req.Actions) == 0 || len(req.Actions) > 10 {
		http.Error(w, "Min 1 action, Max 10 actions in one API call\n", 400)
		return []byte("")
	}

	//ConsistentId
	var anchorid string
	anchorid = req.AnchorID
	bigioDebugf(c, "AnchorId=%s", anchorid)
	//if anchorid exits, meaning consistent is turn on
	//check if we did any action decision for this session before
	//if yes, we return the old result.
	if len(anchorid) > 0 {
		anchorKey := fmt.Sprintf("pact:%s:%s:%s", projectID, sessionID, anchorid)
		prevResult, keyexist := cacheGet(anchorKey)
		if keyexist {
			bigioDebugf(c, "found previous action result for %s", anchorid)
			//set the cache expire for another 30 mins.
			cacheSetExpire30mins(anchorKey)
			//return prevResult and 200
			return []byte(prevResult)
		}
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

	var truePosterior float64

	truePosterior, predStatus, predError := mutantClassify(projectID, goal, states, startDate, endDate)
	bigioDebugf(c, "CLASSIFY truePosterior=%f", truePosterior)

	var baselineReward = 0.0
	var expectedRewards = make([]float64, len(req.Actions)) //this for active learning
	var actResults = make([]actionResult, len(req.Actions))
	var actionsStringArray = make([]string, len(req.Actions))
	var actPredictStatus = make([]bool, len(req.Actions))

	var pickedByActive string
	var pickedIndex int
	var pickedMode string
	//var probArray []float64

	var res *actionResponse

	for i, action := range req.Actions {
		actionsStringArray[i] = action.Name
		actPredictStatus[i] = false
	}

	if predStatus == 0 && truePosterior > 0 {
		var waitgrp sync.WaitGroup
		waitgrp.Add(len(req.Actions))
		//for each action we need to consider
		for i, act := range req.Actions {
			bigioDebugf(c, "Query Action=%s Cost=%f i=%d", act.Name, act.Cost, i)
			//goroutine closure
			go func(i int, act action) {
				defer waitgrp.Done()
				lift := 0.0
				gain := 0.0
				atruePosterior, status, errorMsg := mutantActionify(projectID, act.Name, goal, states, startDate, endDate)

				if status == 0 {
					actPredictStatus[i] = true
					lift = (atruePosterior - truePosterior) / truePosterior * 100      //show lift in percentage
					gain = atruePosterior*(reward-act.Cost) - (truePosterior * reward) //show gain in expected value minus cost_of_action
					baselineReward = truePosterior * reward
					expectedRewards[i] = atruePosterior * (reward - act.Cost) //for active learning
				}

				actResults[i] = actionResult{
					Goal:        goal,
					Action:      act.Name,
					Probability: truePosterior,
					ActTrue:     atruePosterior,
					Lift:        lift,
					Gain:        gain,
					Error:       bigioError{Code: status, Message: errorMsg}}

			}(i, act)
		}
		// wait for goroutines to finish
		waitgrp.Wait()

		//Active Learning
		bigioDebugf(c, "expectedRewards: %v", expectedRewards)
		pickedIndex, pickedMode = activeLearning(req.Explore, actPredictStatus, expectedRewards, baselineReward, gainonly)
		if pickedIndex >= 0 {
			pickedByActive = actionsStringArray[pickedIndex]
		} else {
			pickedByActive = "no-action"
		}

		var actResultsPointer = make([]actionResult, 0)
		if req.Detail == true {
			actResultsPointer = actResults
		}
		res = &actionResponse{
			Results:  actResultsPointer,
			Goal:     goal,
			Decision: pickedByActive,
			Mode:     pickedMode,
		}

	} else {
		//when there is no goal event found, we always explore (learn)
		pickedIndex, _ = alwaysExplorePolicy(actPredictStatus, expectedRewards, baselineReward, gainonly)
		pickedByActive = actionsStringArray[pickedIndex]

		bigioDebugf(c, "alwaysExplore predStatus=0")
		res = &actionResponse{
			Results:  actResults,
			Goal:     goal,
			Decision: pickedByActive,
			Mode:     "learn-goal",
			Error:    bigioError{Code: predStatus, Message: predError},
		}
	}

	returnBody, _ := json.Marshal(res)

	if len(anchorid) > 0 {
		anchorKey := fmt.Sprintf("pact:%s:%s:%s", projectID, sessionID, anchorid)
		cacheSet30mins(anchorKey, string(returnBody))
	}

	if req.MuteActionRecord == false {
		var evt pubSubEvent
		evt.ProjectID = projectID //this is from URL wildcard pattern
		evt.ID = sessionID
		evt.Timestamp = time.Now().UnixNano() / 1000000

		evt.SsnConf = r.Context().Value("ssnConf").(string)
		evt.EventName = pickedByActive
		evt.EventType = "ACT"
		evt.Label = label
		err := fireEvent(evt)
		if err != nil {
			//log.Errorf(c, "failed to record action")
			bigioErrorf(c, "failed to record action")
		} else {
			bigioDebugf(c, "action recorded")
		}
	}

	return returnBody
}

func actionHandlerPOST(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "unsupported HTTP verb", 400)
		return
	}

	//c := appengine.NewContext(r)
	c := context.Background()
	//ctx, _ := appengine.Namespace(c, datastoreNamespace) // remove
	//	ctx_bg := context.Background()
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576)) //max json input 1MB
	if err != nil {
		http.Error(w, "json payload too large, max 1 MByte\n", 400) //413
		return
	}
	bigioDebugf(c, "%s", body)

	returnBody := actionHandlerCommon(w, r, body)
	w.Write(returnBody)
}

func actionHandlerGET(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "unsupported HTTP verb", 400)
		return
	}

	//c := appengine.NewContext(r)
	c := context.Background()
	//ctx, _ := appengine.Namespace(c, datastoreNamespace) //remove

	//get it from Querystring "data=", base64 encoded
	base64Str := r.URL.Query().Get("data")
	body, err := base64.StdEncoding.DecodeString(base64Str)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	bigioDebugf(c, "%s", body)

	returnBody := actionHandlerCommon(w, r, body)
	w.Write(returnBody)
}
