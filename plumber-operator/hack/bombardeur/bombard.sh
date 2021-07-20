i=0
while true
do
  echo -n "{\"msg\": \"hello from bombardeur $i: $(date)\"}" | kafka-console-producer -topic=knative-ingress-0 -brokers=my-cluster-kafka-bootstrap.kafka:9092
  ((i++))
  sleep 1
done
echo "produced a total of $i messages"
