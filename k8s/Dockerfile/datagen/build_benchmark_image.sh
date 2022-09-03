#!/usr/bin/env bash

docker build -t byzer/byzer-lang-k8s-aws:3.1.1-2.3.2-20220817 \
-f ./datagen.Dockerfile \
.