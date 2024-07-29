FROM golang:1.22 AS builder

WORKDIR /app

COPY . .

RUN go mod tidy
RUN go build -o server

FROM golang:1.22
COPY --from=builder /app/server /server
RUN chmod +x /server
CMD ["/server"]