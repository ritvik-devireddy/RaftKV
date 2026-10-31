# Build stage: static binaries, no CGO.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/raftkv-server ./cmd/raftkv-server \
 && CGO_ENABLED=0 go build -trimpath -o /out/raftkv-cli ./cmd/raftkv-cli

# Runtime stage: tiny image with a shell for `docker exec`.
FROM alpine:3.20
COPY --from=build /out/raftkv-server /out/raftkv-cli /usr/local/bin/
VOLUME /data
EXPOSE 7000
ENTRYPOINT ["raftkv-server"]
