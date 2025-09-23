FROM golang:1.25-alpine
WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .

ENV PORT=8000
EXPOSE 8000

RUN CGO_ENABLED=0 GOOS=linux go build -o proxy-service ./main.go

CMD ["./proxy-service"]
