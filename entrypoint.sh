#!/bin/bash

# cd to working directory
cd /app

virtualenv="/app/.venv"
# Rebuild venv environment on startup
if [ -d "$virtualenv" ]; then
    rm -r .venv/*
fi

python3 -m venv $virtualenv
. $virtualenv/bin/activate
$virtualenv/bin/pip install -r requirements.txt

# Create tables
$virtualenv/bin/python3 /app/db.py

# Insert data values
$virtualenv/bin/python3 /app/etl.py

sleep 10

# Startup Flask to keep the container alive
# not really being used at all for now 
$virtualenv/bin/python3 /app/flask-test.py
