#!/usr/bin/env bash
# plumber-operator: a Kubernetes operator to simplify the building and management of stateless stream processing pipelines
#Copyright (C) 2021 Bert Verstraete
#
#This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
#
#This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.

set -eo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

echo "Setting up Kafka"
source "$DIR/kafka-setup/kafka.sh"

#DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
#echo "Setting up observability components"
#source $DIR/observability/observability.sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
echo "Setting up necessary topics for ingress / egress"
source "$DIR/test-topics/topics.sh"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
echo "Setting up keda"
source "$DIR/keda-setup/keda.sh"

echo "Done"
