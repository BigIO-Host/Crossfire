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

go/bin/utils $1 $2 $3
