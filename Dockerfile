FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod ./
COPY main.go index.html ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/switchboard .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/switchboard /switchboard
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/switchboard"]
