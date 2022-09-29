#!/usr/bin/env bash

echo Building docker image
#
## Arguments
#
#PARAMS=""
#while (( "$#" )); do
#  case "$1" in
#    -s|--skip-tests)
#      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
#        SKIP_TESTS=$2
#        shift 2
#      else
#        echo "Error: Argument for $1 is missing" >&2
#        exit 1
#      fi
#      ;;
#    -*|--*=) # unsupported flags
#      echo "Error: Unsupported flag $1" >&2
#      exit 1
#      ;;
#    *) # preserve positional arguments
#      PARAMS="$PARAMS $1"
#      shift
#      ;;
#  esac
#done
## set positional arguments in their proper place
#eval set -- "$PARAMS"
#
#echo Skipping tests : ${SKIP_TESTS:=false}

# Back
VERSION_JAR=$(mvn -q \
    -Dexec.executable=echo \
    -Dexec.args='${project.version}' \
    --non-recursive \
    exec:exec)
JAR_IMAGE_NAME=registry.okina.fr/mobiiti/kishar:"${VERSION_JAR}"

# Maven job is done by Jenkins
#mvn spring-boot:build-image -Dspring-boot.build-image.imageName="${JAR_IMAGE_NAME}" -D${SKIP_TESTS}
#mvn clean package -D${SKIP_TESTS}

docker build -t "${JAR_IMAGE_NAME}" --build-arg JAR_FILE=target/anshar-${VERSION_JAR}.jar .
docker push "${JAR_IMAGE_NAME}"
