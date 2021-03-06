#!/usr/bin/env bash

set -e

if [[ $# -ne 8 ]]; then
    echo "Usage: ./run_pipeline.sh"
    echo "  <user> <pipeline-configuration-json>"
    echo "  <coda-pull-credentials-path> <coda-push-credentials-path> <avf-bucket-credentials-path>"
    echo "  <coda-tools-root> <data-root> <data-backup-dir>"
    echo "Runs the pipeline end-to-end (data fetch, coda fetch, output generation, Drive upload, Coda upload, data backup)"
    exit
fi

USER=$1
PIPELINE_CONFIGURATION_FILE_PATH=$2
CODA_PULL_CREDENTIALS_PATH=$3
CODA_PUSH_CREDENTIALS_PATH=$4
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$5
CODA_TOOLS_ROOT=$6
DATA_ROOT=$7
DATA_BACKUPS_DIR=$8

./1_coda_get.sh "$CODA_PULL_CREDENTIALS_PATH" "$CODA_TOOLS_ROOT" "$DATA_ROOT"

./2_fetch_raw_data.sh "$USER" "$PIPELINE_CONFIGURATION_FILE_PATH" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$DATA_ROOT"

./3_generate_outputs.sh "$USER" "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$DATA_ROOT"

./4_coda_add.sh "$CODA_PUSH_CREDENTIALS_PATH" "$CODA_TOOLS_ROOT" "$DATA_ROOT"

# Backup the project data directory
DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
HASH=$(git rev-parse HEAD)
mkdir -p "$DATA_BACKUPS_DIR"
find "$DATA_ROOT" -type f -name '.DS_Store' -delete
cd "$DATA_ROOT"
tar -czvf "$DATA_BACKUPS_DIR/data-$DATE-$HASH.tar.gzip" .
