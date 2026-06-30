# Builds the raftkv live visualizer (cmd/raftviz) as a single static container.
# Works on any Docker host - Render, Fly.io, Hugging Face Spaces (Docker SDK), etc.
# The server listens on $PORT (default 7860).

# golang:1 auto-downloads the toolchain pinned in go.mod (1.26.x) if the base is older.
FROM golang:1 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /raftviz ./cmd/raftviz

FROM gcr.io/distroless/static-debian12
COPY --from=build /raftviz /raftviz
EXPOSE 7860
ENTRYPOINT ["/raftviz"]
