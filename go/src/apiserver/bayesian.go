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
	"cloud.google.com/go/datastore"
	"fmt"
	"math"
	"time"

	"context"
)

// this matches datastore column name
type row struct {
	Success   int64
	Total     int64
	Timestamp int64
}

type predictDetail struct {
	predictors  []string
	priorT      float64
	priorF      float64
	eventTLikes []float64
	eventFLikes []float64
	evtLength   int
}

func getDatestamps(window int64) (string, string) {
	now := time.Now()
	starttime := now.Add(-time.Duration(24*(window-1)) * time.Hour)

	return starttime.Format("20060102"), now.Format("20060102")
}

func getRecKey(evtString string, setString string) string {
	return fmt.Sprintf("%s::%s", evtString, setString)
}

func mutantClassify(projectID string, goal string, events []string, startDate string, endDate string) (float64, int64, string) {

	var (
		truePosterior float64
		status        int64
		errorMsg      string
	)
	truePosterior, status, errorMsg, _ = mutantClassifyDetailed(projectID, goal, events, startDate, endDate)

	return truePosterior, status, errorMsg

}

//truePosterior, _, status, error_msg, skipEvents, priorT, priorF, skipEventTLikes, skipEventFLikes, evtLength //old
//truePosterior, _, status, error_msg, predictors, priorT, priorF, eventTLikes, eventFLikes, evtLength
func mutantClassifyDetailed(projectID string, goal string, events []string, startDate string, endDate string) (float64, int64, string, predictDetail) {
	c := context.Background()
	var logPt float64
	var logPf float64
	var detail predictDetail

	evtCnt := len(events)

	detail.eventTLikes = make([]float64, evtCnt)
	detail.eventFLikes = make([]float64, evtCnt)
	detail.predictors = events
	detail.evtLength = evtCnt

	//logPt, logPf = getClassPrior(projectID, goal, startDate, endDate)
	logPt, logPf = getClassPriorCached(projectID, goal, startDate, endDate)
	detail.priorT = logPt //save a copy, so we can send feedback pubsub msg
	detail.priorF = logPf //save a copy, so we can send feedback pubsub msg
	// funny error handling. Fix me
	// we never seen the successful goal before
	if logPt == 0 {
		return -1.0, 1, "No Goal event can be referenced. Reason: 101", detail
	}

	bigioDebugf(c, "prior logPt=%f prior logPf=%f", logPt, logPf)

	var eventAvailable int64
	eventAvailable = 0

	bigioDebugf(c, "num of events: %d", evtCnt)
	for index, event := range events {
		//for reach attribute we get the EventProbability to the goal
		var logPTrue, logPFalse float64
		//logPTrue, logPFalse = getEventProbability(projectID, goal, event, startDate, endDate)
		logPTrue, logPFalse = getEventProbabilityCached(projectID, goal, event, startDate, endDate)
		if logPTrue != 0 { // event available

			bigioDebugf(c, "evtLogPt(%s)=%f evtLogPf(%s)=%f", event, logPTrue, event, logPFalse)
			logPt += logPTrue
			logPf += logPFalse

			detail.eventTLikes[index] = logPTrue  //save a copy, so we can send feedback pubsub msg
			detail.eventFLikes[index] = logPFalse //save a copy, so we can send feedback pubsub msg
			eventAvailable++
		} else {
			detail.eventTLikes[index] = -1.0 //save a copy, so we can send feedback pubsub msg
			detail.eventFLikes[index] = -1.0 //save a copy, so we can send feedback pubsub msg
			bigioDebugf(c, "oops, event %s", event)
		}
	}

	var trueP, falseP float64
	bigioDebugf(c, "logPt=%f logPf=%f ", logPt, logPf)
	bigioDebugf(c, "num of cls-events avail: %d", eventAvailable)
	if eventAvailable > 0 {
		//calculate posterior
		trueP = math.Exp(logPt)
		falseP = math.Exp(logPf)
	} else {
		trueP = 0
		falseP = 0
		// all events submitted are missing
		// funny error handling. Fix me
		return -1.0, 1, "All predictor event(s) are not recognized. Reason: 102", detail
	}

	var truePosterior, falsePosterior float64

	truePosterior = trueP / (trueP + falseP)
	falsePosterior = falseP / (trueP + falseP)
	bigioDebugf(c, "truePosterior=%f falsePosterior=%f", truePosterior, falsePosterior)

	//return format is: true-prob, false-prob, status, error_message, skipEvents, prior, skipEventLikes, evtLength
	return truePosterior, 0, "", detail
}

func getClassPriorCached(projectID string, goal string, startDate string, endDate string) (float64, float64) {
	c := context.Background()
	key := fmt.Sprintf("prior:%s:%s:%s:%s", projectID, goal, startDate, endDate)

	result, keyexist := cacheGet(key)
	if keyexist {
		bigioDebugf(c, "cache hit k:%s, v:%s", key, result)
		// strconv.ParseFloat(result, 64) note: prevent truncate error

		var r1, r2 float64
		fmt.Sscanf(result, "%f,%f", &r1, &r2)
		return r1, r2
	}
	//log.Debugf(c, "cache miss k:%s", key)

	r1, r2 := getClassPrior(projectID, goal, startDate, endDate)
	// strconv.FormatFloat(r1, 'f', -1, 64) right way to do, Fix me

	cacheSetVari(key, fmt.Sprintf("%f,%f", r1, r2))
	return r1, r2
}

func getEventProbabilityCached(projectID string, goal string, event string, startDate string, endDate string) (float64, float64) {
	key := fmt.Sprintf("evtp:%s:%s:%s:%s:%s", projectID, goal, event, startDate, endDate)

	result, keyexist := cacheGet(key)
	if keyexist {

		var r1, r2 float64
		fmt.Sscanf(result, "%f,%f", &r1, &r2)
		return r1, r2
	}

	r1, r2 := getEventProbability(projectID, goal, event, startDate, endDate)
	cacheSetVari(key, fmt.Sprintf("%f,%f", r1, r2))
	return r1, r2

}

func getClassPrior(projectID string, goal string, startDate string, endDate string) (float64, float64) {
	c := context.Background()
	/* currently 2 calls to datastore per response */
	bigioDebugf(c, "get Prior date scan range: %s-%s", startDate, endDate)

	var total, success int64
	total = 0
	success = 0

	successC := make(chan int64)
	totalC := make(chan int64)

	go func() {
		reckey1 := getRecKey("GOAL#"+goal, "[TS]")
		retSuccess, _ := getFromDataStoreCached(projectID, reckey1, startDate, endDate)
		successC <- retSuccess
	}()

	go func() {
		reckey2 := getRecKey("[AL]", "[TS]")
		_, retTotal := getFromDataStoreCached(projectID, reckey2, startDate, endDate)
		totalC <- retTotal
	}()

	for i := 0; i < 2; i++ {
		select {
		case success = <-successC:
		case total = <-totalC:
		}
	}

	// if success is 0, means we never saw this goal success before, no prior
	// if total is 0, means we never saw this traffic before!

	// if success > 0 && total > 0 {
	if total > 10 {
		//var vt, vf float64
		var v float64
		if success > 0 {
			// added Laplacian smoothing. 1/1000
			//vt = (float64(success) + 1.0) / (float64(total) + 1000.0)
			//vf = (float64(total-success) + 1.0) / (float64(total) + 1000.0)
			v = float64(success) / float64(total)
		} else {
			v = 1 / 1000
		}

		bigioDebugf(c, "T-Prior = %f", v)

		//problem: what if v >= 1. log of negative number is NaN.
		//data processing error?
		if v >= 1 {
			bigioWarningf(c, "warning %f is too lage", v)
			v = 0.999
		} else if v == 0 { //problem is v is zero, log(0) is -Inf
			v = 1e-9
		}
		return math.Log(v), math.Log(1 - v)
	}

	// log function never return zero, so we use zero value as exception
	return 0, 0
}

func getEventProbability(projectID string, goal string, event string, startDate string, endDate string) (float64, float64) {
	c := context.Background()
	/* currently 4 calls to datastore per event within a response */
	bigioDebugf(c, "get eventP(%s) date scan range: %s-%s", event, startDate, endDate)

	var total, success, evtAll, evtTotal int64
	success = 0
	total = 0
	evtAll = 0
	evtTotal = 0

	successC := make(chan int64)
	totalC := make(chan int64)
	evtAllC := make(chan int64)
	evtTotalC := make(chan int64)

	go func() {
		reckey1 := getRecKey("GOAL#"+goal, event)
		retSuccess, _ := getFromDataStoreCached(projectID, reckey1, startDate, endDate)
		successC <- retSuccess
	}()

	go func() {
		reckey2 := getRecKey("GOAL#"+goal, "[TE]")
		_, retTotal := getFromDataStoreCached(projectID, reckey2, startDate, endDate)
		totalC <- retTotal
	}()
	//log.Debugf(c, "total: %d", total)

	go func() {
		reckey3 := getRecKey("[AL]", event)
		_, retEvtAll := getFromDataStoreCached(projectID, reckey3, startDate, endDate)
		evtAllC <- retEvtAll
	}()

	go func() {
		reckey4 := getRecKey("[AL]", "[TE]")
		_, retEvtTotal := getFromDataStoreCached(projectID, reckey4, startDate, endDate)
		evtTotalC <- retEvtTotal
	}()
	//log.Debugf(c, "evtTotal: %d", evtTotal)

	for i := 0; i < 4; i++ {
		select {
		case success = <-successC:
		case total = <-totalC:
		case evtAll = <-evtAllC:
		case evtTotal = <-evtTotalC:
		}
	}

	//combine
	if total > 0 && evtAll > 0 {
		var v1, v2 float64
		// added Laplacian smoothing. 1/100
		v1 = (float64(success) + 1.0) / (float64(total) + 100.0)
		v2 = (float64(evtAll-success) + 1.0) / (float64(evtTotal-total) + 100.0)
		bigioDebugf(c, "eventP(%s) = %f, eventP(%s) = %f", event, v1, event, v2)
		if v1 <= 0 {
			bigioWarningf(c, "event v1 warning: %f", v2)
			v1 = 1e-9
		}
		if v2 <= 0 {
			bigioWarningf(c, "event v2 warning: %f", v2)
			v2 = 1e-9
		}
		return math.Log(v1), math.Log(v2)
	}

	// log function never return zero, so we use zero value as exception
	return 0, 0

}

func mutantActionify(projectID string, action string, goal string, events []string, startDate string, endDate string) (float64, int64, string) {

	var (
		truePosterior float64
		status        int64
		errorMsg      string
	)
	truePosterior, status, errorMsg, _ = mutantActionifyDetailed(projectID, action, goal, events, startDate, endDate)

	return truePosterior, status, errorMsg

}

//atruePosterior, _, status, error_msg, skipEvents, priorT, priorF, skipEventTLikes, skipEventFLikes, evtLength //old
//atruePosterior, _, status, error_msg, predictors, priorT, priorF, eventTLikes, eventFLikes, evtLength
func mutantActionifyDetailed(projectID string, action string, goal string, events []string, startDate string, endDate string) (float64, int64, string, predictDetail) {
	c := context.Background()
	var logPt float64
	var logPf float64
	var detail predictDetail

	evtCnt := len(events)

	detail.eventTLikes = make([]float64, evtCnt)
	detail.eventFLikes = make([]float64, evtCnt)
	detail.predictors = events
	detail.evtLength = evtCnt

	/* use ACT prior */
	//logPt, logPf = getActionClassPrior(action, goal, startDate, endDate)
	//logPt, logPf = getActionClassPriorCached(action, goal, startDate, endDate)
	/* use PRED prior */
	//logPt, logPf = getClassPrior(projectID, goal, startDate, endDate)
	logPt, logPf = getClassPriorCached(projectID, goal, startDate, endDate)

	detail.priorT = logPt //save a copy, so we can send feedback pubsub msg
	detail.priorF = logPf //save a copy, so we can send feedback pubsub msg
	// funny error handling. Fix me
	// we never seen the successful goal before
	if logPt == 0 {
		return -1.0, 1, "No Goal event can be referenced. Reason: 201", detail
	}

	bigioDebugf(c, "prior logPt=%f prior logPf=%f", logPt, logPf)

	var eventAvailable int64
	eventAvailable = 0

	bigioDebugf(c, "event count: %d", evtCnt)
	for index, event := range events {
		//for reach attribute we get the EventProbability to the goal
		var logPTrue, logPFalse float64
		//logPTrue, logPFalse = getActionEventProbability(projectID, action, goal, event, startDate, endDate, c)
		logPTrue, logPFalse = getActionEventProbabilityCached(projectID, action, goal, event, startDate, endDate)
		if logPTrue != 0 { // event available

			bigioDebugf(c, "evtLogPt(%s)=%f evtLogPf(%s)=%f", event, logPTrue, event, logPFalse)
			logPt += logPTrue
			logPf += logPFalse

			detail.eventTLikes[index] = logPTrue  //save a copy, so we can send feedback pubsub msg
			detail.eventFLikes[index] = logPFalse //save a copy, so we can send feedback pubsub msg
			eventAvailable++
		} else {
			detail.eventTLikes[index] = -1.0 //save a copy, so we can send feedback pubsub msg
			detail.eventFLikes[index] = -1.0 //save a copy, so we can send feedback pubsub msg
			bigioDebugf(c, "oops, event %s", event)
		}
	}

	var trueP, falseP float64
	bigioDebugf(c, "logPt=%f logPf=%f ", logPt, logPf)
	bigioDebugf(c, "num of act-events avail: %d", eventAvailable)
	if eventAvailable > 0 {
		//calculate posterior
		trueP = math.Exp(logPt)
		falseP = math.Exp(logPf)
	} else {
		trueP = 0
		falseP = 0
		// all events submitted are missing
		// funny error handling. Fix me
		return -1.0, 1, "All predictor event(s) are not recognized. Reason: 202", detail
	}

	var truePosterior, falsePosterior float64

	truePosterior = trueP / (trueP + falseP)
	falsePosterior = falseP / (trueP + falseP)
	bigioDebugf(c, "truePosterior=%f falsePosterior=%f", truePosterior, falsePosterior)

	//return format is: true-prob, false-prob, status, error_message, skipEvents, prior, skipEventLikes, evtLength
	return truePosterior, 0, "", detail
}

func getActionEventProbabilityCached(projectID string, action string, goal string, event string, startDate string, endDate string) (float64, float64) {
	key := fmt.Sprintf("actevtp:%s:%s:%s:%s:%s:%s", projectID, action, goal, event, startDate, endDate)

	result, keyexist := cacheGet(key)
	if keyexist {
		var r1, r2 float64
		fmt.Sscanf(result, "%f,%f", &r1, &r2)
		return r1, r2
	}

	r1, r2 := getActionEventProbability(projectID, action, goal, event, startDate, endDate)
	cacheSetVari(key, fmt.Sprintf("%f,%f", r1, r2))
	return r1, r2

}

func getActionEventProbability(projectID string, action string, goal string, event string, startDate string, endDate string) (float64, float64) {
	c := context.Background()
	/* currently 4 calls to datastore per event within a response */
	bigioDebugf(c, "get eventP(%s) date scan range: %s-%s", event, startDate, endDate)

	var total, success, evtAll, evtTotal int64

	success = 0
	total = 0
	evtAll = 0
	evtTotal = 0

	successC := make(chan int64)
	totalC := make(chan int64)
	evtAllC := make(chan int64)
	evtTotalC := make(chan int64)

	go func() {
		reckey1 := fmt.Sprintf("GOAL#%s::ACT#%s::%s", goal, action, event)
		retSuccess, _ := getFromDataStoreCached(projectID, reckey1, startDate, endDate)
		successC <- retSuccess
	}()

	go func() {
		reckey2 := fmt.Sprintf("GOAL#%s::ACT#%s::%s", goal, action, "[TE]")
		_, retTotal := getFromDataStoreCached(projectID, reckey2, startDate, endDate)
		totalC <- retTotal
	}()

	go func() {
		reckey3 := fmt.Sprintf("%s::ACT#%s::%s", "[AL]", action, event)
		_, retEvtAll := getFromDataStoreCached(projectID, reckey3, startDate, endDate)
		evtAllC <- retEvtAll
	}()

	go func() {
		reckey4 := fmt.Sprintf("%s::ACT#%s::%s", "[AL]", action, "[TE]")
		_, retEvtTotal := getFromDataStoreCached(projectID, reckey4, startDate, endDate)
		evtTotalC <- retEvtTotal
	}()

	for i := 0; i < 4; i++ {
		select {
		case success = <-successC:
		case total = <-totalC:
		case evtAll = <-evtAllC:
		case evtTotal = <-evtTotalC:
		}
	}

	//combine
	if total > 0 && evtAll > 0 {
		var v1, v2 float64
		// added Laplacian smoothing. 1/100
		v1 = (float64(success) + 1.0) / (float64(total) + 100.0)
		v2 = (float64(evtAll-success) + 1.0) / (float64(evtTotal-total) + 100.0)
		bigioDebugf(c, "eventP(%s) = %f, eventP(%s) = %f", event, v1, event, v2)
		if v1 < 0 {
			bigioWarningf(c, "event v2 warning: %f", v2)
			v1 = 1e-9
		}
		if v2 < 0 {
			bigioWarningf(c, "event v2 warning: %f", v2)
			v2 = 1e-9
		}
		return math.Log(v1), math.Log(v2)
	}

	// log function never return zero, so we use zero value as exception
	return 0, 0

}

//Not in use
func getActionClassPrior(projectID string, action string, goal string, startDate string, endDate string) (float64, float64) {
	c := context.Background()
	/* currently 2 calls to datastore per response */
	bigioDebugf(c, "get Prior date scan range: %s-%s", startDate, endDate)

	var total, success int64
	success = 0
	total = 0

	successC := make(chan int64)
	totalC := make(chan int64)

	go func() {
		reckey1 := fmt.Sprintf("%s::%s::%s", goal, action, "[TS]")
		retSuccess, _ := getFromDataStoreCached(projectID, reckey1, startDate, endDate)
		successC <- retSuccess
	}()

	go func() {
		reckey2 := fmt.Sprintf("%s::%s::%s", "[AL]", action, "[TS]")
		_, retTotal := getFromDataStoreCached(projectID, reckey2, startDate, endDate)
		totalC <- retTotal
	}()

	for i := 0; i < 2; i++ {
		select {
		case success = <-successC:
		case total = <-totalC:
		}
	}

	// if success is 0, means we never saw this goal success before, no prior
	// if total is 0, means we never saw this traffic before!

	// if success > 0 && total > 0 {
	if total > 10 {
		//var vt, vf float64
		var v float64
		if success > 0 {
			// added Laplacian smoothing. 1/1000
			//vt = (float64(success) + 1.0) / (float64(total) + 1000.0)
			//vf = (float64(total-success) + 1.0) / (float64(total) + 1000.0)
			v = float64(success) / float64(total)
		} else {
			v = 1 / 1000
		}

		//log.Debugf(c, "T-Prior = %f F-Prior = %f", vt, vf)
		bigioDebugf(c, "T-Prior = %f", v)

		//problem: what if v >= 1. log of negative number is NaN.
		//data processing error?
		if v >= 1 {
			bigioWarningf(c, "warning %f is too lage", v)
			v = 0.999
		} else if v == 0 { //problem is v is zero, log(0) is -Inf
			v = 1e-9
		}

		return math.Log(v), math.Log(1 - v)
	}

	// log function never return zero, so we use zero value as exception
	return 0, 0

}

//Not in use
func getActionClassPriorCached(projectID string, action string, goal string, startDate string, endDate string) (float64, float64) {
	c := context.Background()
	key := fmt.Sprintf("actprior:%s:%s:%s:%s:%s", projectID, action, goal, startDate, endDate)

	result, keyexist := cacheGet(key)
	if keyexist {
		bigioDebugf(c, "cache hit k:%s, v:%s", key, result)
		// strconv.ParseFloat(result, 64) note: prevent truncate error

		var r1, r2 float64
		fmt.Sscanf(result, "%f,%f", &r1, &r2)
		return r1, r2
	}

	//log.Debugf(c, "cache miss k:%s", key)

	r1, r2 := getActionClassPrior(projectID, action, goal, startDate, endDate)
	// strconv.FormatFloat(r1, 'f', -1, 64) right way to do, Fix me

	cacheSetVari(key, fmt.Sprintf("%f,%f", r1, r2))
	return r1, r2

}

func getFromDataStoreCached(projectID string, recKey string, startDate string, endDate string) (int64, int64) {
	key := fmt.Sprintf("element:%s:%s:%s:%s", projectID, recKey, startDate, endDate)

	result, keyexist := cacheGet(key)
	if keyexist {
		//log.Debugf(c, "element cache hit k:%s, v:%s", key, result)

		var r1, r2 int64
		fmt.Sscanf(result, "%d,%d", &r1, &r2)
		return r1, r2
	}

	//log.Debugf(c, "element cache miss k:%s", key)

	r1, r2 := getFromDataStore(projectID, recKey, startDate, endDate)
	cacheSet(key, fmt.Sprintf("%d,%d", r1, r2))
	return r1, r2

}

func getFromDataStore(projectID string, recKey string, startDate string, endDate string) (int64, int64) {
	c := context.Background()
	startKey := fmt.Sprintf("%s::%s", recKey, startDate)
	endKey := fmt.Sprintf("%s::%s", recKey, endDate)
	projDatastoreTable := fmt.Sprintf("%s-%s", datastoreTable, projectID)

	//log.Debugf(c, "startkey:%s, endkey:%s, table:%s", startKey, endKey, projDatastoreTable)

	//q := datastore.NewQuery(projDatastoreTable).
	//	Filter("__key__ >=", datastore.NewKey(c, projDatastoreTable, startKey, 0, nil)).
	//	Filter("__key__ <=", datastore.NewKey(c, projDatastoreTable, endKey, 0, nil))

	startDSKey := datastore.NameKey(projDatastoreTable, startKey, nil)
	endDSKey := datastore.NameKey(projDatastoreTable, endKey, nil)
	startDSKey.Namespace = datastoreNamespace
	endDSKey.Namespace = datastoreNamespace

	q := datastore.NewQuery(projDatastoreTable).Namespace(datastoreNamespace).
		Filter("__key__ >=", startDSKey).
		Filter("__key__ <=", endDSKey)

	var rows []row

	//if _, err := q.GetAll(c, &rows); err != nil {
	if _, err := datastoreClient.GetAll(c, q, &rows); err != nil {
		//log.Errorf(c, "error when fetching from datastore: %s", err)
		bigioErrorf(c, "error when fetching from datastore: %s", err)
	}
	//log.Debugf(c, "datastore rows: %v", rows)

	var total, success int64
	total = 0
	success = 0

	for _, row := range rows {
		total += row.Total
		success += row.Success
	}

	return success, total
}
