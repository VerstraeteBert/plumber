### Builder
FROM golang:1.15.8-alpine3.12 AS builder
WORKDIR /app
ADD go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o ./bin/plumber-sidecar .
### Runtime
FROM alpine:3.12.2
WORKDIR /app
COPY --from=builder /app/bin/plumber-sidecar ./
ENTRYPOINT [ "./plumber-sidecar" ]
