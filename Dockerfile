ARG GO_IMAGE=docker.io/library/golang:1.27.1-bookworm@sha256:8d48e12ec56735e9358640898b9d9b9fcca110612ed8a5567438c0a1baa24e66
FROM ${GO_IMAGE} AS build
ARG TARGETOS=linux
ARG TARGETARCH
ARG VERSION=development
ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOENV=off GOWORK=off \
    GOPROXY=https://proxy.golang.org GOSUMDB=sum.golang.org \
    GOPRIVATE= GONOPROXY= GONOSUMDB= \
    GOMODCACHE=/tmp/modules GOCACHE=/tmp/buildcache
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -buildvcs=false -ldflags="-s -w -X main.version=${VERSION}" -o /out/streamd ./cmd/streamd \
    && mkdir -p /out/data

FROM scratch
ARG VERSION=development
LABEL org.opencontainers.image.title="streamd" \
      org.opencontainers.image.version="${VERSION}"
COPY --from=build /out/streamd /streamd
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=build --chown=65532:65532 /out/data /data
COPY LICENSE /LICENSE
USER 65532:65532
WORKDIR /data
EXPOSE 1935 8080
STOPSIGNAL SIGTERM
ENTRYPOINT ["/streamd"]
CMD ["-config", "/etc/streamd/streamd.yaml"]
