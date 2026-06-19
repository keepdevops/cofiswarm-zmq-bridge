# Multi-stage build for the cofiswarm-zmq-bridge sidecar.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./cmd/cofiswarm-zmq-bridge

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/app /usr/local/bin/cofiswarm-zmq-bridge
USER app
EXPOSE 5555
ENTRYPOINT ["/usr/local/bin/cofiswarm-zmq-bridge"]
