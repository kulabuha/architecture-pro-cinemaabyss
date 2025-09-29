FROM golang:1.25-alpine
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV PORT=8082
EXPOSE 8082

RUN CGO_ENABLED=0 GOOS=linux go build -o events-service ./main.go

CMD ["./events-service"]
