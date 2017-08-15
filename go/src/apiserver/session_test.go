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

import "testing"
import "strings"

// test with redis docker image
// $ docker run --name some-redis -p 192.168.99.100:6379:6379/tcp -d redis
const redisServer string = "192.168.99.100:6379"
const redisPasswd string = ""

const testId string = "testSessionId"
const testProjectID string = "projectXDDD"
const testEventType string = "evt"
const testTimestamp int64 = 1463135360

func TestSessionStore(t *testing.T) {

	var testData = []string{"EVENT#bigio", "ATR#host", "EVENT#pulse", "EVENT#LOL", "ATR#XDDD", "EVENT#bigio"}
	var verify = []string{"EVENT#bigio", "EVENT#pulse", "EVENT#LOL", "EVENT#bigio"}

	store := newSessionStore(redisServer, redisPasswd)
	store.clear(testProjectID, testId, testEventType)

	insertTestData(store, testData)

	values := store.dump(testProjectID, testId, testEventType)
	for i, v := range values {
		t.Logf("%d %s %s", i, v, verify[i])
		if v != verify[i] {
			t.Errorf("Except %s in index %d, got %s", verify[i], i, v)
		}
	}

	store.clear(testProjectID, testId, testEventType)
	empty_result := store.dump(testProjectID, testId, testEventType)
	length := len(empty_result)
	t.Logf("length after clear: %d", length)
	if length > 0 {
		t.Errorf("Except a empty array, but got %d element", length)
	}
}

func TestMergeStates(t *testing.T) {
	store := newSessionStore(redisServer, redisPasswd)
	store.clearAll(testProjectID, testId)

	var testData = []string{"EVENT#bigio", "ATR#host", "EVENT#pulse", "EVENT#LOL", "ATR#XDDD", "EVENT#bigio"}
	var states = []string{"EVENT#bigio", "EVENT#evt001"}

	var verify = []string{"EVENT#bigio", "EVENT#pulse", "EVENT#LOL", "ATR#host", "ATR#XDDD", "EVENT#evt001"}

	insertTestData(store, testData)
	values := store.mergeStates(testProjectID, testId, 10, states)

	for i, v := range values {
		if v != verify[i] {
			t.Errorf("Except %s in index %d, got %s", verify[i], i, v)
		}
	}

	store.clearAll(testProjectID, testId)

}

func TestMaxSessionKept(t *testing.T) {
	store := newSessionStore(redisServer, redisPasswd)
	max_sessions := store.getMaxSessionsKept()
	store.clear(testProjectID, testId, testEventType)
	randomInsert(store, testProjectID, testId, max_sessions)

	length := len(store.dump(testProjectID, testId, testEventType))
	t.Logf("length before: %d", length)

	randomInsert(store, testProjectID, testId, 20)
	length_after := len(store.dump(testProjectID, testId, testEventType))
	t.Logf("length after: %d", length_after)

	if int64(length_after) != max_sessions {
		t.Errorf("except %d sessions stored, but got %d", max_sessions, length_after)
	}

	store.clear(testProjectID, testId, testEventType)
}

// TODO: test for TTL

func randomInsert(store *redisSessionStore, projectId string, id string, count int64) {
	for i := int64(0); i < count; i++ {
		evt := pubSubEvent{
			ProjectID: projectId,
			ID:        id,
			EventName: "LOL",
			EventType: "EVENT",
			Timestamp: testTimestamp,
		}
		store.append(&evt)
	}
}

func insertTestData(store *redisSessionStore, testData []string) {
	for _, v := range testData {
		s := strings.Split(v, "#")
		evtType, evtName := s[0], s[1]
		evt := pubSubEvent{
			ProjectID: testProjectID,
			ID:        testId,
			EventName: evtName,
			EventType: evtType,
			Timestamp: testTimestamp,
		}
		store.append(&evt)
	}
}
