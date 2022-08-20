#!/usr/bin/env bash

kubectl create rolebinding default-role-binding --role=byzer-admin --serviceaccount=flag-qa:default --namespace=flag-qa