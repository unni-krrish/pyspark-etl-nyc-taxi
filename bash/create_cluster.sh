#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: ./create_cluster.sh bucket-name region zone"
    exit
fi

PROJECT=$(gcloud config get-value project)
BUCKET=$1
REGION=$2
ZONE=$3
INSTALL=gs://$BUCKET/install.sh

#upload install.sh
gsutil cp install.sh $INSTALL

gcloud dataproc clusters create my-cluster2 \
    --num-workers=2 \
    --worker-machine-type=n1-standard-2 \
    --worker-boot-disk-size=100 \
    --master-machine-type=n1-standard-2 \
    --master-boot-disk-size=100 \
    --image-version=2.0-ubuntu18 \
    --enable-component-gateway \
    --optional-components=JUPYTER \
    --zone=$ZONE \
    --region=$REGION \
    --initialization-actions=$INSTALL 