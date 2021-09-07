#! /bin/bash

for i in {1..20}
do
     # clean up bench
    kubectl delete topology --all
    kubectl delete topologypart --all
    kubectl delete deployment --all
    kubectl delete pod --all
    sleep 5
    kubectl delete -f ./k8s/topics.yaml
    sleep 10
    echo "starting bench num $i"
    # set up bench
    kubectl apply -f ./k8s/topics.yaml
    sleep 3
    kubectl apply -f ./k8s/receiver.yaml
    kubectl apply -f ./k8s/$1/topopart-pre.yaml
    sleep 5
    kubectl apply -f ./k8s/$1/topopart-post.yaml
    kubectl apply -f ./k8s/topo-pre.yaml
    sleep 5
    kubectl apply -f ./k8s/sender.yaml
    sleep 40
    # perform upgrade
    kubectl apply -f ./k8s/topo-post.yaml
    sleep 60
    # collect results
    kubectl logs receiver > ./data/$1/bench_$i.txt
done
