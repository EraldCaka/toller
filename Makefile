obu:
	@go build -o bin/obu obu/main.go
	@./bin/obu
receiver:
	@go build -o bin/data_receiver data_receiver/main.go
	@./bin/data_receiver
.PHONY: obu