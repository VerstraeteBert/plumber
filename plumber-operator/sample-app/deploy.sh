set -eo pipefail
docker build . -t verstraetebert/plumber-greeter:v0.0.1
docker push verstraetebert/plumber-greeter:v0.0.1
