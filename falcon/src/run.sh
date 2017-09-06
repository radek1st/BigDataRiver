#!/bin/bash
set -e
pip install -r /project/requirements.txt
cd /project/src && gunicorn -b 0.0.0.0:8000 bdr:app
