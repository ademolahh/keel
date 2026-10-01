FROM golang:1.26.5-alpine3.23 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /raftkv ./cmd/kv

FROM alpine:3.23

COPY --from=build /raftkv /raftkv

ENTRYPOINT ["/raftkv"]
