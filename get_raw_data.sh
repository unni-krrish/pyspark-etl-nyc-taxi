#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: ./get_raw_data.sh url gcs_path"
    exit
fi

URL=$1
DEST=$2
FNAME="${URL##*/}"

# Download to Disk
wget $URL

#upload install.sh
gsutil cp FNAME $DEST