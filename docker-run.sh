#!/bin/bash

set -e

IMAGE_NAME=icraf

while [[ $# -gt 0 ]]; do
    case "$1" in 
        --profile-cpu)
            PROFILE_CPU=true
            CPU_PROFILE_OUTPUT_PATH="$2"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

# Check that the correct number of arguments were provided.
if [[ $# -ne 12 ]]; then
    echo "Usage: ./docker-run.sh
    [--profile-cpu <profile-output-path>]
    <user> <pipeline-configuration-file-path> <google-cloud-credentials-file-path> <phone-number-uuid-table-path>
    <raw-data-dir> <prev-coded-dir> <json-output-path> <icr-output-dir> <coded-output-dir> <messages-output-csv>
    <individuals-output-csv> <production-output-csv> <advert-phone-numbers-csv>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$2
INPUT_PHONE_UUID_TABLE=$3
INPUT_RAW_DATA_DIR=$4
PREV_CODED_DIR=$5
OUTPUT_JSON=$6
OUTPUT_ICR_DIR=$7
OUTPUT_AUTO_CODED_DIR=$8
OUTPUT_MESSAGES_CSV=$9
OUTPUT_INDIVIDUALS_CSV=${10}
OUTPUT_PRODUCTION_CSV=${11}
OUTPUT_ADVERT_PHONE_NUMBERS_CSV=${12}

# Build an image for this pipeline stage.
docker build --build-arg INSTALL_CPU_PROFILER="$PROFILE_CPU" -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
# When run, the container will:
#  - Copy the service account credentials from the google cloud storage url 'SERVICE_ACCOUNT_CREDENTIALS_URL',
#    if the --drive-upload flag has been set.
#    The google cloud storage access is authorised via volume mounting (-v in the docker container create command).
#  - Run the pipeline.
# The gcloud bucket access is authorised via volume mounting (-v in the docker container create command)
if [[ "$PROFILE_CPU" = true ]]; then
    PROFILE_CPU_CMD="pyflame -o /data/cpu.prof -t"
    SYS_PTRACE_CAPABILITY="--cap-add SYS_PTRACE"
fi
CMD="pipenv run $PROFILE_CPU_CMD python -u pipeline.py \
    \"$USER\" configurations/pipeline_config.json /credentials/google-cloud-credentials.json \
    /data/phone-number-uuid-table-input.json /data/raw-data /data/prev-coded \
    /data/output.json /data/output-icr /data/coded /data/output-messages.csv \
    /data/output-individuals.csv /data/output-production.csv /data/advert-phone-numbers.csv"

container="$(docker container create ${SYS_PTRACE_CAPABILITY} -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"

function finish {
    # Tear down the container when done.
    docker container rm "$container" >/dev/null
}
trap finish EXIT

# Copy input data into the container
docker cp "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$container:/credentials/google-cloud-credentials.json"
docker cp "$INPUT_PHONE_UUID_TABLE" "$container:/data/phone-number-uuid-table-input.json"
docker cp "$INPUT_RAW_DATA_DIR" "$container:/data/raw-data"
if [[ -d "$PREV_CODED_DIR" ]]; then
    docker cp "$PREV_CODED_DIR" "$container:/data/prev-coded"
fi

# Run the container
docker start -a -i "$container"

# Copy the output data back out of the container
echo "copying traced data from '$container:/data/output.json' to '$OUTPUT_JSON'"
mkdir -p "$(dirname "$OUTPUT_JSON")"
docker cp "$container:/data/output.json" "$OUTPUT_JSON"

echo "Copying ICR files from '$container:/data/output-icr/' to '$OUTPUT_ICR_DIR'"
mkdir -p "$OUTPUT_ICR_DIR"
docker cp "$container:/data/output-icr/." "$OUTPUT_ICR_DIR"

echo "copying coded datasets from '$container:/data/coded/' to '$OUTPUT_AUTO_CODED_DIR'"
mkdir -p "$OUTPUT_AUTO_CODED_DIR"
docker cp "$container:/data/coded/." "$OUTPUT_AUTO_CODED_DIR"

echo "copying production csv from '$container:/data/output-production.csv' to '$OUTPUT_PRODUCTION_CSV'"
mkdir -p "$(dirname "$OUTPUT_PRODUCTION_CSV")"
docker cp "$container:/data/output-production.csv" "$OUTPUT_PRODUCTION_CSV"

echo "copying advert csv from '$container:/data/advert-phone-numbers.csv' to '$OUTPUT_ADVERT_PHONE_NUMBERS_CSV'"
mkdir -p "$(dirname "$OUTPUT_ADVERT_PHONE_NUMBERS_CSV")"
docker cp "$container:/data/advert-phone-numbers.csv" "$OUTPUT_ADVERT_PHONE_NUMBERS_CSV"

echo "copying by-individual csv from '$container:/data/output-individuals.csv' to '$OUTPUT_INDIVIDUALS_CSV'"
mkdir -p "$(dirname "$OUTPUT_INDIVIDUALS_CSV")"
docker cp "$container:/data/output-individuals.csv" "$OUTPUT_INDIVIDUALS_CSV"

echo "copying by-message csv from '$container:/data/output-messages.csv' to '$OUTPUT_MESSAGES_CSV'"
mkdir -p "$(dirname "$OUTPUT_MESSAGES_CSV")"
docker cp "$container:/data/output-messages.csv" "$OUTPUT_MESSAGES_CSV"

if [[ "$PROFILE_CPU" = true ]]; then
    mkdir -p "$(dirname "$CPU_PROFILE_OUTPUT_PATH")"
    docker cp "$container:/data/cpu.prof" "$CPU_PROFILE_OUTPUT_PATH"
fi
