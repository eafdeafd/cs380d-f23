#!/bin/bash

sudo docker image rm $(sudo docker image ls --format '{{.Repository}} {{.ID}}' | grep 'eafdeafd' | awk '{print $2}')

cd dockerfiles

sudo docker build . -f base.dockerfile -t eafdeafd/kvs:base --network=host
sudo docker push eafdeafd/kvs:base

sudo docker build . -f client.dockerfile -t eafdeafd/kvs:client --network=host
sudo docker push eafdeafd/kvs:client

sudo docker build . -f frontend.dockerfile -t eafdeafd/kvs:frontend --network=host
sudo docker push eafdeafd/kvs:frontend

sudo docker build . -f server.dockerfile -t eafdeafd/kvs:server --network=host
sudo docker push eafdeafd/kvs:server

cd ..
