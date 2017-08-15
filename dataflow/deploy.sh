# usage:
# ./deploy.sh [dataflow job options]
# eg. update the job:#
# ./deploy.sh --update

CONFFILE=${BIGIO_CONF:-"config.env"}
if [ ! -f "../$CONFFILE" ]; then
    echo "ERROR: $CONFFILE not exist"
    exit
fi



source ../$CONFFILE

mvn compile exec:java \
-Dexec.mainClass=host.bigio.ml.MainPipeline \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args="--project=$GAE_PROJECT_ID \
--stagingLocation=$DATAFLOW_STAGE_LOCATION \
--runner=DataflowRunner \
--streaming=true \
--jobName=MainPipeline \
--numWorkers=$DATAFLOW_NUM_WORKERS \
--maxNumWorkers=$DATAFLOW_MAX_NUM_WORKERS \
--eventSubscription=projects/$GAE_PROJECT_ID/subscriptions/$EVENT_PUBSUB_SUBSCRIPTION \
--datastoreNamespace=$DATASTORE_NAMESPACE \
--datastoreTable=$DATASTORE_TABLE_PREFIX \
--sessionConfs=$SESSION_CONFS \
--sessionConfDefault=$SESSION_CONF_DEFAULT \
$@"
