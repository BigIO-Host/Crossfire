#!/bin/bash

# Default configuration file name is config.env
# Override it with BIGIO_CONF environment variable
# BIGIO_CONF=config.env.dev ./genconf.sh

CONFFILE=${BIGIO_CONF:-"./config.env"}
if [ ! -f "$CONFFILE" ]; then
    echo "ERROR: $CONFFILE not exist"
    exit
fi
echo "BIGIO_CONF is set to: $CONFFILE"

set -o allexport
source $CONFFILE
set +o allexport

echo "PROJECT_ID is set to: $GAE_PROJECT_ID"

declare -a arr=("go/src/apiserver/app.yaml")

function gen_file {
    echo "$1.dist -> $1"
    envsubst < $1.dist > $1
}

function clean_file {
    echo "rm $1"
    rm $1
}

for i in "${arr[@]}"
do
  if [[ "$1" == "-c" ]]; then
     clean_file "$i"
  else
     gen_file "$i"
  fi
done
