#!/usr/bin/env bash
# plumber-operator: a Kubernetes operator to simplify the building and management of stateless stream processing pipelines
#Copyright (C) 2021 Bert Verstraete
#
#This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
#
#This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

kubectl create namespace plumber-kafka

kubectl create -f 'https://strimzi.io/install/latest?namespace=plumber-kafka' -n plumber-kafka

kubectl wait --for=condition=Established --all crd
kubectl wait pod --timeout=-1s --for=condition=Ready -l '!job-name' -n plumber-kafka

kubectl apply -f "$DIR/kafka-single-ephemeral.yaml"
kubectl wait pod --timeout=-1s --for=condition=Ready -l '!job-name' -n plumber-kafka
