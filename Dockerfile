# syntax=docker/dockerfile:1

# One image definition for every binary in cmd/. Pick it with the CMD build arg:
#   docker build --build-arg CMD=api .        (default)
#   docker build --build-arg CMD=ledger-api .
# Every binary must support a `healthcheck` subcommand.

# ---- build stage ----
FROM golang:1.26-alpine AS build
ARG CMD=api
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

# ---- runtime stage ----
# Distroless static: no shell, no package manager, runs as uid 65532.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 CMD ["/app", "healthcheck"]
ENTRYPOINT ["/app"]
