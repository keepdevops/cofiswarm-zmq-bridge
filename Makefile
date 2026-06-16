ROLE := zmq-bridge
.PHONY: build test test-standalone-layout
build:
	go build -o bin/cofiswarm-zmq-bridge ./cmd/cofiswarm-zmq-bridge
test: build test-standalone-layout test-gate
test-standalone-layout:
	./test/scripts/assert-layout.sh $(ROLE)
test-gate:
	./test/scripts/test-gate.sh
