FROM golang:1.24.1-alpine AS builder

ENV GOOS=linux
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o main .

FROM chromedp/headless-shell:132.0.6834.111 AS installer
RUN apt-get update && apt install ca-certificates -y
WORKDIR /app
COPY --from=builder /build/main /app/
COPY --from=builder /build/config.yaml /app/config.yaml
ENTRYPOINT ["/app/main"]
