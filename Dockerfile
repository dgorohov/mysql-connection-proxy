FROM golang:alpine as builder
RUN apk add ca-certificates git
ENV GO111MODULE=on
RUN mkdir /build
ADD . /build/
WORKDIR /build
RUN go mod download && \
    CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-s -w -extldflags "-static"' -o main .

FROM scratch
COPY --from=builder /build/main /app/
VOLUME /data
WORKDIR /app

CMD ["./main"]
