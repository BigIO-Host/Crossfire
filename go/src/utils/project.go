package main

import (
	"cloud.google.com/go/datastore"
	"common"
	"golang.org/x/net/context"
	"log"
	"os"
	"time"
)

func mustGetenv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("Fail to get enviornmnet variable %s", k)
	}

	return v
}

func addProject(projectID string, ssnConf string) {

	ctx := context.Background()
	datastoreClient, err := datastore.NewClient(ctx, mustGetenv("GAE_PROJECT_ID"))
	if err != nil {
		log.Fatalf("Failed to create Datastore client: %v", err)
	}

	key := datastore.NameKey(mustGetenv("DATASTORE_PROJECTS_TABLE"), projectID, nil)
	key.Namespace = mustGetenv("DATASTORE_NAMESPACE")

	pStat := new(common.ProjectStatusModel)

	pStat.ProjectID = projectID
	pStat.SsnConf = ssnConf
	pStat.AuthPulse = "F"
	pStat.AuthPredict = "F"
	pStat.AuthAction = "F"
	pStat.CreateDate = time.Now()
	pStat.Deleted = "F"

	if _, err := datastoreClient.Put(ctx, key, pStat); err != nil {
		log.Fatalf("fail to put datastore entry: %v", err)
	}

	log.Printf("Project %s created", projectID)

}

func main() {

	if len(os.Args) < 4 {
		log.Fatalf("usage: utils addproject [project ID] [session setting]")
	}

	addProject(os.Args[2], os.Args[3])

}
