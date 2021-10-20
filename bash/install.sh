#!/bin/bash
# Install py files on all the nodes if necessary
apt-get-update
apt-get-install -y python-pip
pip install pipenv