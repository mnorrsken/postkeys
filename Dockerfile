ARG BUILDPLATFORM
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o postkeys ./cmd/server

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /app/postkeys /app/postkeys

USER 1000:1000

EXPOSE 6379

ENTRYPOINT ["/app/postkeys"]
