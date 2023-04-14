#!/bin/bash

set -e

mkdir -p data

# Uncomment the following lines for a larger data set
# curl https://edu.postgrespro.com/demo-big-en.zip --output ./demo-big-en.zip
# unzip demo-big-en.zip 


curl https://edu.postgrespro.com/demo-small-en.zip --output ./data/demo-small-en.zip

unzip ./data/demo-small-en.zip -d data

cat ./data/demo-small-en-20170815.sql > ./data/init.sql

rm -f ./data/demo-small-en-20170815.sql
