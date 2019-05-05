#!/bin/bash
echo "building"
mvn clean package

mkdir odps-data-carrier

mkdir -p odps-data-carrier/bin
cp bin/* odps-data-carrier/bin/

mkdir -p odps-data-carrier/res
cp -r resources/* odps-data-carrier/res/

mkdir -p odps-data-carrier/libs
cp data-transfer-hive-udtf/target/*-jar-with-dependencies.jar odps-data-carrier/libs/
cp meta-carrier/target/*-jar-with-dependencies.jar odps-data-carrier/libs/
cp meta-processor/target/*-jar-with-dependencies.jar odps-data-carrier/libs/

zip -r odps-data-carrier.zip odps-data-carrier

rm -r odps-data-carrier
