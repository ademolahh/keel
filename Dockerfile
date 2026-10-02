FROM golang:1.26.5-alpine3.23 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /keel ./cmd/keel

FROM alpine:3.23

COPY --from=build /keel /keel

ENTRYPOINT ["/keel"]
