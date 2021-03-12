#!/usr/bin/env bash
docker-compose -f docker-compose.yml -f docker-compose.sentinel.yml down
docker-compose -f docker-compose.yml -f docker-compose.sentinel.yml build
docker-compose -f docker-compose.yml -f docker-compose.sentinel.yml up --remove-orphans
