#!/usr/bin/env bash
kubectl create namespace keda
kubectl apply -f "https://github.com/kedacore/keda/releases/download/v2.2.0/keda-2.2.0.yaml"
kubectl wait pod --timeout=-1s --for=condition=Ready -l '!job-name' -n keda
