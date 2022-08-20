#!/usr/bin/env bash

docker build -t byzer/eks-tpcds-datagen:2.4.5_2.12_0.0.1 \
-f ./eks-tpcds-data-gen-2.4.5_2.12.Dockerfile \
.