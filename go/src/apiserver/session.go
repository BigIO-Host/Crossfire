// Copyright 2017 BigIO.host. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"gopkg.in/redis.v5"
	"os"
	"time"
)

const defaultTTL string = "1800s"

const defaultMaxSessionsKept int64 = 50

type redisSessionStore struct {
	redisClient        *redis.Client
	redisClusterClient *redis.ClusterClient
	redisClusterMode   bool
	ttl                time.Duration
	maxSessionsKept    int64
}

func newSessionStore(redisAddr string, redisPwd string) *redisSessionStore {
	client := redis.NewClient(&redis.Options{
		Network:  "tcp4",
		Addr:     redisAddr,
		Password: redisPwd,
		DB:       0,
	})

	ttl, _ := time.ParseDuration(defaultTTL)

	return &redisSessionStore{
		redisClient:      client,
		redisClusterMode: false,
		ttl:              ttl,
		maxSessionsKept:  defaultMaxSessionsKept,
	}

}

func errorLog(msg string) {
	fmt.Fprintf(os.Stderr, msg)
}

func makeRedisKey(projectID string, id string, eventType string) string {
	return fmt.Sprintf("%s:%s:%s", projectID, id, eventType)
}

func (s *redisSessionStore) getMaxSessionsKept() int64 {
	return s.maxSessionsKept
}

func (s *redisSessionStore) append(evt *pubSubEvent) {

	var eventType string

	switch evt.EventType {
	case "EVENT":
		eventType = "evt"
	case "ATR":
		eventType = "atr"
	default:
		eventType = ""
	}

	// insert into the redis list, projectid+sessionid as key
	redisKey := makeRedisKey(evt.ProjectID, evt.ID, eventType)

	// maintain max sessions stored
	if s.redisClient.LLen(redisKey).Val() >= s.maxSessionsKept {
		r2 := s.redisClient.LTrim(redisKey, 1, s.maxSessionsKept)
		if r2.Err() != nil {
			fmt.Printf("[SessionStore] append error when LTrim: %v", r2.Err())
		}
	}

	combinedName := fmt.Sprintf("%s#%s", evt.EventType, evt.EventName)

	r := s.redisClient.RPush(redisKey, combinedName) // insert eventType#eventName
	if r.Err() != nil {
		fmt.Printf("[SessionStore] append error when RPush: %v", r.Err())
	}

	// maintain max sessions stored
	//r2 := s.redisClient.LTrim(redisKey, 0, s.maxSessionsKept-1) // is this a bug?

	r3 := s.redisClient.Expire(redisKey, s.ttl)
	if r3.Err() != nil {
		fmt.Printf("[SessionStore] append error when set TTL: %v", r3.Err())
	}

}

func (s *redisSessionStore) mergeStates(projectID string, sessionID string, ctxLength int64, states []string) []string {
	return removeDuplicates(
		append(
			append(s.dumpLast(projectID, sessionID, "evt", ctxLength),
				s.dumpLast(projectID, sessionID, "atr", ctxLength)...),
			states...))
}

func (s *redisSessionStore) dump(projectID string, sessionID string, eventType string) []string {
	redisKey := makeRedisKey(projectID, sessionID, eventType)
	r := s.redisClient.LRange(redisKey, 0, s.maxSessionsKept)
	if r.Err() != nil {
		fmt.Printf("[SessionStore] Dump error when LRange: %v", r.Err())
	}
	return r.Val()
}

func (s *redisSessionStore) dumpLast(projectID string, sessionID string, eventType string, lastN int64) []string {
	redisKey := makeRedisKey(projectID, sessionID, eventType)
	//r := s.redisClient.LRange(redisKey, -1*lastN, lastN) //-1 means last element
	r := s.redisClient.LRange(redisKey, -1*lastN, -1) //-1 means last element
	if r.Err() != nil {
		fmt.Printf("[SessionStore] Dump error when LRange: %v", r.Err())
	}
	return r.Val()
}

func (s *redisSessionStore) clear(projectID string, sessionID string, eventType string) {
	redisKey := makeRedisKey(projectID, sessionID, eventType)
	r := s.redisClient.Del(redisKey)
	if r.Err() != nil {
		fmt.Printf("[SessionStore] clear error: %v", r.Err())
	}
}

func (s *redisSessionStore) clearAll(projectID string, sessionID string) {
	s.clear(projectID, sessionID, "evt")
	s.clear(projectID, sessionID, "atr")
}

func removeDuplicates(elements []string) []string {
	// Use map to record duplicates as we find them.
	encountered := map[string]bool{}
	result := []string{}

	for v := range elements {
		if encountered[elements[v]] == true {
			// Do not add duplicate.
		} else {
			// Record this element as an encountered element.
			encountered[elements[v]] = true
			// Append to result slice.
			result = append(result, elements[v])
		}
	}
	// Return the new slice.
	return result
}
