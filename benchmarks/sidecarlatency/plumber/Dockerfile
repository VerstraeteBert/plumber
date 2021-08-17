### Builder
FROM golang:1.15.8-alpine3.12 AS builder
WORKDIR /app
COPY . .
RUN go build -o ./bin/sidecar-tester .
### Runtime
FROM alpine:3.12.2
WORKDIR /app
COPY --from=builder /app/bin/sidecar-tester ./
ENTRYPOINT [ "./sidecar-tester" ]
