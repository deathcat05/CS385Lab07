#!/bin/bash

echo "0" > database.txt

python3 updatedb.py & python3 updatedb.py
