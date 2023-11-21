FROM golang:1.21.4-alpine3.17

WORKDIR /go/src/app

COPY go.mod .
COPY go.sum .

COPY . .

RUN go mod download

RUN go install -v main.go

CMD ["main"]