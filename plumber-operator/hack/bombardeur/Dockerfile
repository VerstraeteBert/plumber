FROM golang
COPY bombard.sh /bombard.sh
RUN go get github.com/Shopify/sarama/tools/kafka-console-producer
RUN chmod +x /bombard.sh
ENTRYPOINT ["tail", "-f", "/dev/null"]
