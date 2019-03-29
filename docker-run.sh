#!/bin/bash

set -e

IMAGE_NAME=icraf

while [[ $# -gt 0 ]]; do
    case "$1" in 
        --profile-cpu)
            PROFILE_CPU=true
            CPU_PROFILE_OUTPUT_PATH="$2"
            shift 2;;
        --drive-upload)
            DRIVE_UPLOAD=true

            DRIVE_SERVICE_ACCOUNT_CREDENTIALS_URL=$2
            MESSAGES_DRIVE_PATH=$3
            INDIVIDUALS_DRIVE_PATH=$4
            PRODUCTION_DRIVE_PATH=$5
            shift 5;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

# Check that the correct number of arguments were provided.
if [[ $# -ne 18 ]]; then
    echo "Usage: ./docker-run.sh
    [--profile-cpu <profile-output-path>]
    [--drive-upload <drive-auth-file> <messages-drive-path> <individuals-drive-path> <production-drive-path>]
    <user> <google-cloud-credentials-file-path> <phone-number-uuid-table-path>
    <icraf-s01e01-input-path> <icraf-s01e02-input-path> <icraf-s01e03-input-path> <icraf-s01e04-input-path>
    <icraf-s01e05-input-path> <icraf-s01e06-input-path> <icraf-demog-input-path> <icraf-follow-up-survey-input-path> 
    <prev-coded-dir> <json-output-path> <icr-output-dir> <coded-output-dir> <messages-output-csv> <individuals-output-csv>
    <production-output-csv>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
GOOGLE_CLOUD_CREDENTIALS_FILE_PATH=$2
INPUT_PHONE_UUID_TABLE=$3
INPUT_S01E01=$4
INPUT_S01E02=$5
INPUT_S01E03=$6
INPUT_S01E04=$7
INPUT_S01E05=$8
INPUT_S01E06=$9
INPUT_S01_DEMOG=${10}
INPUT_FOLLOW_UP_SURVEY=${11}
PREV_CODED_DIR=${12}
OUTPUT_JSON=${13}
OUTPUT_ICR_DIR=${14}
OUTPUT_AUTO_CODED_DIR=${15}
OUTPUT_MESSAGES_CSV=${16}
OUTPUT_INDIVIDUALS_CSV=${17}
OUTPUT_PRODUCTION_CSV=${18}

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
if [[ "$DRIVE_UPLOAD" = true ]]; then
    DRIVE_UPLOAD_ARG="--drive-upload /root/.config/drive-service-account-credentials.json \"$MESSAGES_DRIVE_PATH\" \"$INDIVIDUALS_DRIVE_PATH\" \"$PRODUCTION_DRIVE_PATH\""
fi
CMD="pipenv run $PROFILE_CPU_CMD python -u pipeline.py $DRIVE_UPLOAD_ARG \
    \"$USER\" configurations/pipeline_config.json /credentials/google-cloud-credentials.json /data/phone-number-uuid-table-input.json \
    /data/icraf-s01e01-input.json  /data/icraf-s01e02-input.json  /data/icraf-s01e03-input.json \
    /data/icraf-s01e04-input.json  /data/icraf-s01e05-input.json  /data/icraf-s01e06-input.json \
    /data/icraf-demog-input.json /data/icraf-follow-up-survey-input.json /data/prev-coded \
    /data/output.json /data/output-icr /data/coded \
    /data/output-messages.csv /data/output-individuals.csv /data/output-production.csv \
"
container="$(docker container create ${SYS_PTRACE_CAPABILITY} -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"

function finish {
    # Tear down the container when done.
    docker container rm "$container" >/dev/null
}
trap finish EXIT

# Copy input data into the container
docker cp "$GOOGLE_CLOUD_CREDENTIALS_FILE_PATH" "$container:/credentials/google-cloud-credentials.json"
docker cp "$INPUT_PHONE_UUID_TABLE" "$container:/data/phone-number-uuid-table-input.json"
docker cp "$INPUT_S01E01" "$container:/data/icraf-s01e01-input.json"
docker cp "$INPUT_S01E02" "$container:/data/icraf-s01e02-input.json"
docker cp "$INPUT_S01E03" "$container:/data/icraf-s01e03-input.json"
docker cp "$INPUT_S01E04" "$container:/data/icraf-s01e04-input.json"
docker cp "$INPUT_S01E05" "$container:/data/icraf-s01e05-input.json"
docker cp "$INPUT_S01E06" "$container:/data/icraf-s01e06-input.json"
docker cp "$INPUT_S01_DEMOG" "$container:/data/icraf-demog-input.json"
docker cp "$INPUT_FOLLOW_UP_SURVEY" "$container:/data/icraf-follow-up-survey-input.json"
if [[ -d "$PREV_CODED_DIR" ]]; then
    docker cp "$PREV_CODED_DIR" "$container:/data/prev-coded"
fi

# Run the container
docker start -a -i "$container"

# Copy the output data back out of the container
mkdir -p "$(dirname "$OUTPUT_JSON")"
docker cp "$container:/data/output.json" "$OUTPUT_JSON"

mkdir -p "$OUTPUT_ICR_DIR"
docker cp "$container:/data/output-icr/." "$OUTPUT_ICR_DIR"

mkdir -p "$OUTPUT_AUTO_CODED_DIR"
docker cp "$container:/data/coded/." "$OUTPUT_AUTO_CODED_DIR"

mkdir -p "$(dirname "$OUTPUT_MESSAGES_CSV")"
docker cp "$container:/data/output-production.csv" "$OUTPUT_MESSAGES_CSV"

mkdir -p "$(dirname "$OUTPUT_INDIVIDUALS_CSV")"
docker cp "$container:/data/output-individuals.csv" "$OUTPUT_INDIVIDUALS_CSV"

mkdir -p "$(dirname "$OUTPUT_PRODUCTION_CSV")"
docker cp "$container:/data/output-production.csv" "$OUTPUT_PRODUCTION_CSV"

if [[ "$PROFILE_CPU" = true ]]; then
    mkdir -p "$(dirname "$CPU_PROFILE_OUTPUT_PATH")"
    docker cp "$container:/data/cpu.prof" "$CPU_PROFILE_OUTPUT_PATH"
fi
