FROM golang:1.22 AS builder

WORKDIR /app

COPY . .

RUN go mod tidy
RUN go build -o worker

FROM golang:1.22
COPY --from=builder /app/worker /worker
RUN chmod +x /worker
CMD ["/worker"]