FROM golang:alpine

RUN apk add --no-cache ca-certificates ffmpeg

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY src/main.go .

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o main main.go

CMD ["./main"]