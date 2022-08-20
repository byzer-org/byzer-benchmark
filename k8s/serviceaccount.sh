#!/usr/bin/env bash

kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: default
  namespace: flag-qa
EOF