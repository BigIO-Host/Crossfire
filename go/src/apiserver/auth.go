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
	gorillacontext "github.com/gorilla/context"
	"github.com/gorilla/mux"
	"golang.org/x/net/context"
	"time"

	"encoding/json"
	"net/http"

	"common"
)

type projectStatusContextCache struct {
	ProjectStatus *common.ProjectStatusModel `json:projectStatus`
	Timestamp     int64                      `json:timestamp`
}

const apikeyIndex string = "api_key"

type handler func(w http.ResponseWriter, r *http.Request)

func authHandler(pass handler, perm string) handler {

	return func(w http.ResponseWriter, r *http.Request) {
		//c := appengine.NewContext(r)
		c := context.Background()
		//ctx, _ := appengine.Namespace(c, datastoreNamespace) //remove

		// get projectID from URL
		projectID := mux.Vars(r)["projectId"]
		bigioDebugf(c, string(projectID))

		// check project status
		httpStatus, httpMsg, needAuth, sessionConf := checkprojectStatus(c, projectID, perm)
		if httpStatus != 200 {
			bigioDebugf(c, "httpStatus: %d, msg: %s", httpStatus, httpMsg)
			http.Error(w, httpMsg, httpStatus)
			return
		}
		if needAuth {
			//log.Debugf(c, "authKey provided: %s", r.Header.Get("authKey"))
			var authkey string
			if r.URL.Query().Get(apikeyIndex) != "" {
				authkey = r.URL.Query().Get(apikeyIndex)
			} else {
				// curl --user "apikey123456:"
				authkey, _, _ = r.BasicAuth()
			}
			httpStatus, httpMsg = verifyprojectKey(c, projectID, authkey, perm)
			if httpStatus != 200 {
				bigioDebugf(c, "httpStatus: %d, msg: %s", httpStatus, httpMsg)
				http.Error(w, httpMsg, httpStatus)
				return
			}
		}

		//gorillacontext.Set(r, "windowSize", int64(windowsize))
		gorillacontext.Set(r, "ssnConf", sessionConf)

		// they are not related to auth, anyway
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		pass(w, r)
	}
}

func getprojectStatus(c context.Context, projectID string) (*common.ProjectStatusModel, error) {

	key := datastore.NameKey(dsProjectsTable, projectID, nil)
	key.Namespace = datastoreNamespace
	pStat := new(common.ProjectStatusModel)
	if err := datastoreClient.Get(c, key, pStat); err != nil {
		bigioDebugf(c, "DatastoreError: %s", err)
		bigioDebugf(c, "project: %s not found", projectID)
		return pStat, err
	}
	return pStat, nil
}

func getprojectStatusCached(c context.Context, projectID string) (*common.ProjectStatusModel, error) {
	key := fmt.Sprintf("projectStatus:%s", projectID)

	projectStatus := new(common.ProjectStatusModel)

	projectStatusJSON, keyexist := cacheGet(key)
	if keyexist {
		json.Unmarshal([]byte(projectStatusJSON), &projectStatus)
		return projectStatus, nil
	}

	projectStatus, err := getprojectStatus(c, projectID)
	if err == nil {
		projectStatusBytes, err := json.Marshal(projectStatus)
		if err == nil {
			cacheSet(key, string(projectStatusBytes))
		}
	}

	return projectStatus, err

}

func getprojectStatusContextCached(c context.Context, projectID string) (*common.ProjectStatusModel, error) {

	key := fmt.Sprintf("projectStatus:%s", projectID)

	cacheContent := new(projectStatusContextCache)
	cacheJSON, keyExist := contextCache[key]
	if keyExist {
		json.Unmarshal([]byte(cacheJSON), &cacheContent)
		if time.Now().Unix()-cacheContent.Timestamp < contextCacheTTL { // Cache hit
			return cacheContent.ProjectStatus, nil
		}
	}

	projectStatus, err := getprojectStatusCached(c, projectID)
	if err == nil {
		cacheJSON, err := json.Marshal(projectStatusContextCache{ProjectStatus: projectStatus, Timestamp: time.Now().Unix()})
		if err == nil {
			contextCache[key] = string(cacheJSON)
		}
	}

	return projectStatus, err

}

// actual usage is this one, it can decode the issue and return message and status_code
func checkprojectStatus(c context.Context, projectID string, apiName string) (int, string, bool, string) {
	projectStatus, err := getprojectStatusContextCached(c, projectID)
	// projectStatus, err := getprojectStatusCached(c, projectID)

	if err != nil || projectStatus.Deleted == "D" {
		//Not found
		return 404, "404 Page not found, check if projectID is correct", true, ""
	}

	var needAuth bool
	if apiName == "AuthPulse" && projectStatus.AuthPulse == "T" {
		needAuth = true
	}
	if apiName == "AuthPredict" && projectStatus.AuthPredict == "T" {
		needAuth = true
	}
	if apiName == "AuthAction" && projectStatus.AuthAction == "T" {
		needAuth = true
	}

	return 200, "OK", needAuth, projectStatus.SsnConf
}

// ### projectKey Verification
func getprojectKey(c context.Context, projectID string, projectKey string, apiName string) string {
	// strkey := fmt.Sprintf("%s:%s:%s", projectID, projectKey, apiName)
	strkey := fmt.Sprintf("%s", projectKey)
	//key := datastore.NewKey(c, dsprojectKeysTable, strkey, 0, nil)
	key := datastore.NameKey(dsProjectKeysTable, strkey, nil)
	key.Namespace = datastoreNamespace
	pKey := new(common.ProjectKeyModel)
	if err := datastoreClient.Get(c, key, pKey); err != nil {
		bigioDebugf(c, "%s", err)
		bigioDebugf(c, "project key: %s not found", strkey)
		return "NA"
	}
	return pKey.Locked
}

func getprojectKeyCached(c context.Context, projectID string, projectKey string, apiName string) string {
	key := fmt.Sprintf("projectKey:%s:%s:%s", projectID, projectKey, apiName)

	result, keyexist := cacheGet(key)
	if keyexist {
		//log.Debugf(c, "key cache hit k:%s, v:%s", key, result)
		var locked string
		//WARNING: the values are "space" delimited.
		fmt.Sscanf(result, "%s", &locked)

		return locked
	}
	//log.Debugf(c, "key cache miss k:%s", key)

	locked := getprojectKey(c, projectID, projectKey, apiName)
	//set redis cache
	cacheSet(key, fmt.Sprintf("%s", locked))
	//set local context cache with creation timstamp
	contextCache[key] = fmt.Sprintf("%s %d", locked, time.Now().Unix())
	return locked
}

func getprojectKeyContextCached(c context.Context, projectID string, projectKey string, apiName string) string {
	var locked string
	var timestamp int64

	key := fmt.Sprintf("projectKey:%s:%s:%s", projectID, projectKey, apiName)

	result := contextCache[key]
	if result != "" {
		//log.Debugf(c, "key context cache hit, value: %s", result)
		fmt.Sscanf(result, "%s %d", &locked, &timestamp)

		if time.Now().Unix()-timestamp < contextCacheTTL {
			//log.Debugf(c, "bingo")
			// early return here
			return locked
		}
		//log.Debugf(c, "but cache is expired: %d", timestamp)
	} else {
		//log.Debugf(c, "key context cache miss")
	}
	// call external cache
	locked = getprojectKeyCached(c, projectID, projectKey, apiName)
	//set local context cache with creation timstamp
	contextCache[key] = fmt.Sprintf("%s %d", locked, time.Now().Unix())

	return locked
}

// actual usage is this one, it can decode the issue and return message and status_code
func verifyprojectKey(c context.Context, projectID string, projectKey string, apiName string) (int, string) {
	locked := getprojectKeyContextCached(c, projectID, projectKey, apiName)

	if locked == "NA" {
		//Forbidden
		return 403, "403 Forbidden, check if projectID or key is correct"
	}
	if locked != "F" {
		//Unauthorized
		return 401, "401 Unauthorized, key is locked or authenication failed"
	}
	return 200, "OK"
}
