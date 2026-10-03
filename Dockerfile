ARG GO_VERSION=1.26.5
ARG ALPINE_VERSION=3.23

FROM --platform=${BUILDPLATFORM} golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS base

WORKDIR /src
ENV CGO_ENABLED=0
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download && go mod verify

COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /keel ./cmd/keel

FROM alpine:3.23

COPY --from=base /keel /keel

ENTRYPOINT ["/keel"]
