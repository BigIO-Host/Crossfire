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
	"gopkg.in/redis.v5"
	"math/rand"
	"time"
)

var (
	redisClient  *redis.Client
	redisTimeout time.Duration
)

func cacheSet(k string, v string) {

	redisClient.Set(k, v, redisTimeout)

}

func cacheSetVari(k string, v string) {
	redisClient.Set(k, v, time.Duration(300+rand.Int63n(60))*time.Second)

}

func cacheSet30mins(k string, v string) {

	redisClient.Set(k, v, time.Duration(1800)*time.Second)

}

func cacheSetExpire30mins(k string) {

	redisClient.Expire(k, time.Duration(1800)*time.Second)

}

func cacheGet(k string) (string, bool) {

	val, err := redisClient.Get(k).Result()
	if err != nil {
		return val, false
	}

	return val, true

}
