# syntax=docker/dockerfile:1.7

ARG NODE_VERSION=22.21.0
ARG GO_VERSION=1.25.3
ARG VERSION=container

FROM --platform=$BUILDPLATFORM node:${NODE_VERSION}-bookworm-slim AS web
WORKDIR /src/webmanager

COPY webmanager/package.json webmanager/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm \
    npm ci --no-audit --no-fund

COPY webmanager/ ./
RUN npm run build-prod

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS build
ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT
ARG VERSION

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY --from=web /src/webmanager/dist/webmanager/browser/ ./internal/assets/browser/

RUN --mount=type=cache,target=/root/.cache/go-build \
    if [ "${TARGETARCH}" = "arm" ]; then export GOARM="${TARGETVARIANT#v}"; fi; \
    CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" \
    go build -trimpath \
      -ldflags="-s -w -X 'github.com/adrianceding/engarde/internal/version.Version=${VERSION}'" \
      -o /out/engarde ./cmd/engarde

FROM scratch
ARG VERSION

LABEL org.opencontainers.image.title="engarde" \
      org.opencontainers.image.description="Multipath TCP SOCKS5 relay" \
      org.opencontainers.image.source="https://github.com/adrianceding/engarde" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.licenses="GPL-2.0-only"

COPY --from=build /out/engarde /usr/local/bin/engarde
COPY LICENSE.txt /licenses/LICENSE.txt

USER 65532:65532
WORKDIR /etc/engarde
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/engarde"]
CMD ["/etc/engarde/engarde.yml"]
