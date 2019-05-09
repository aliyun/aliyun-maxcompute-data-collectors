#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR

mvn clean package

mkdir -p odps-data-carrier

mkdir -p odps-data-carrier/bin
cp bin/* odps-data-carrier/bin/

mkdir -p odps-data-carrier/res
cp -r resources/* odps-data-carrier/res/

mkdir -p odps-data-carrier/libs
cp data-transfer-hive-udtf/target/*-jar-with-dependencies.jar odps-data-carrier/libs/
cp meta-carrier/target/*-jar-with-dependencies.jar odps-data-carrier/libs/
cp meta-processor/target/*-jar-with-dependencies.jar odps-data-carrier/libs/

cp odps_config.ini odps-data-carrier

zip -r odps-data-carrier.zip odps-data-carrier

rm -r odps-data-carrier
