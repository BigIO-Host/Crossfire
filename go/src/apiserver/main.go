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
	"cloud.google.com/go/pubsub"

	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	"golang.org/x/net/context"
	"google.golang.org/appengine"

	"gopkg.in/redis.v5"
	"log"
)

var (
	topic              *pubsub.Topic
	datastoreClient    *datastore.Client
	datastoreTable     string
	datastoreNamespace string
	dsProjectsTable    string
	dsProjectKeysTable string
	contextCacheTTL    int64
	contextCache       map[string]string
	sessionStore       *redisSessionStore
	enableDebug        bool
)

func main() {

	ctx := context.Background()

	//initialize context cache map
	contextCache = make(map[string]string)

	//BigioDebug switch
	enableDebug, _ = strconv.ParseBool(mustGetenv("ENABLE_DEBUG"))

	// Pubsub client
	pubsubClient, err := pubsub.NewClient(ctx, mustGetenv("GCLOUD_PROJECT_NAME"))
	if err != nil {
		log.Fatalf("%v", err)
	}

	topic, err = pubsubClient.CreateTopic(ctx, mustGetenv("PUBSUB_TOPIC"))

	dsProjectsTable = mustGetenv("DATASTORE_PROJECTS_TABLE")
	dsProjectKeysTable = mustGetenv("DATASTORE_PROJECTKEYS_TABLE")

	cacheTTL := mustGetenv("CACHE_TTL")
	contextCacheTTL, _ = strconv.ParseInt(mustGetenv("CONTEXT_CACHE_TTL"), 0, 64)

	redisAddr := mustGetenv("REDIS_SERVER")
	redisPwd := mustGetenv("REDIS_PASSWD")

	redisClient = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPwd,
		DB:       0,
	})
	redisTimeout, _ = time.ParseDuration(cacheTTL + "s")

	// Creates a Datastore client.
	datastoreClient, err = datastore.NewClient(ctx, mustGetenv("GCLOUD_PROJECT_NAME"))
	if err != nil {
		log.Fatalf("Failed to create Datastore client: %v", err)
	}

	datastoreNamespace = mustGetenv("DATASTORE_NAMESPACE")
	datastoreTable = mustGetenv("DATASTORE_TABLE_PREFIX")

	sessionRedisAddr := mustGetenv("SESSION_REDIS_SERVER")
	sessionRedisPwd := mustGetenv("SESSION_REDIS_PASSWD")

	sessionStore = newSessionStore(sessionRedisAddr, sessionRedisPwd)

	rtr := mux.NewRouter()
	rtr.HandleFunc("/v1.0/{projectId:[A-Za-z0-9\\-\\_]+}/pulse/{sessionId:.+}", authHandler(pulseHandlerPOST, "AuthPulse")).Methods("POST")
	rtr.HandleFunc("/v1.0/{projectId:[A-Za-z0-9\\-\\_]+}/pulse/{sessionId:.+}", authHandler(pulseHandlerGET, "AuthPulse")).Methods("GET")

	rtr.HandleFunc("/v1.0/{projectId:[A-Za-z0-9\\-\\_]+}/prediction/{sessionId:.+}", authHandler(classifyHandlerPOST, "AuthPredict")).Methods("POST")
	rtr.HandleFunc("/v1.0/{projectId:[A-Za-z0-9\\-\\_]+}/prediction/{sessionId:.+}", authHandler(classifyHandlerGET, "AuthPredict")).Methods("GET")

	rtr.HandleFunc("/v1.0/{projectId:[A-Za-z0-9\\-\\_]+}/action/{sessionId:.+}", authHandler(actionHandlerPOST, "AuthAction")).Methods("POST")
	rtr.HandleFunc("/v1.0/{projectId:[A-Za-z0-9\\-\\_]+}/action/{sessionId:.+}", authHandler(actionHandlerGET, "AuthAction")).Methods("GET")

	rtr.PathPrefix("/").HandlerFunc(crosHandler).Methods("OPTIONS")

	http.Handle("/", rtr)

	appengine.Main()
}

func crosHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Max-Age", "86400") // 1 day

	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
}

func mustGetenv(k string) string {
	v := os.Getenv(k)
	return v
}
