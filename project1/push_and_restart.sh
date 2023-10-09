#!/bin/bash
git add server.py frontend.py
git commit -m "More testing"
git push
sh ./scripts/delete_pods.sh
sh ./scripts/clean_build_docker.sh