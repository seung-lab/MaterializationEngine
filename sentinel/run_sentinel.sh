#!/bin/bash

if [ $# -eq 0 ]
    then
        echo "One arg is required"
else
    cp "/sentinel/$1.conf" "/sentinel/$1running.conf"
    redis-sentinel "/sentinel/$1running.conf"
fi