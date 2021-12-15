#! /bin/bash

for i in {2..20}
do
    mkdir -p data/$1/$i
    (cd ../../plumber-operator/ && go run main.go 2> ../benchmarks/upgrade/data/$1/$i/operator-timings.txt &) 
    sleep 10
     # clean up bench
    kubectl delete topology --all
    sleep 10
    kubectl delete topologypart --all
    kubectl delete deployment --all
    kubectl delete pod --all
    sleep 10
    kubectl delete -f ./k8s/topics.yaml
    sleep 10
    echo "starting bench num $i"
    # set up bench
    kubectl apply -f ./k8s/topics.yaml
    sleep 5
    kubectl apply -f ./k8s/receiver.yaml
    sleep 5
    kubectl apply -f ./k8s/$1/topopart-pre.yaml
    sleep 5
    kubectl apply -f ./k8s/topo-pre.yaml
    sleep 5
    kubectl apply -f ./k8s/sender.yaml
    sleep 5
    kubectl apply -f ./k8s/$1/topopart-post.yaml
    sleep 40
    # perform upgrade
    echo $(($(date +%s%N)/1000000)) > ./data/$1/$i/user-timings.txt
    kubectl apply -f ./k8s/topo-post.yaml
    sleep 60
    # collect results
    kubectl logs receiver > ./data/$1/$i/message-timings.txt
    kill $(lsof -t -i :8080 -s tcp:LISTEN)
    sleep 10
    grep -o -P '(?<=####).*(?=,)' data/$1/$i/operator-timings.txt > data/$1/$i/operator-timings-filtered.txt
done

kubectl delete topology --all
sleep 10
kubectl delete topologypart --all
kubectl delete deployment --all
kubectl delete pod --all
sleep 10
kubectl delete -f ./k8s/topics.yaml
