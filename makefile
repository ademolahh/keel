.PHONY: gen

gen:
	@protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/raft.proto

print-covr:
	@go test -coverprofile=cover.out ./internal/raft/ && go tool cover -func=cover.out && \
		go tool cover -html=cover.out

stress:
	@for i in 1 2 3; do go test -race -count=1 -timeout 120s ./internal/raft 2>&1 | grep -qE '^ok' && printf "pass " || printf "FAIL "; done; echo
