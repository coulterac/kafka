FROM golang:1.17

# use the default value of docker to denote this was built with Docker, this can be overriden
# by specifying a build-arg in the CI process
ARG BUILD=docker

COPY . /go/src/github.com/coulterac/kafka

RUN go install -ldflags "-X main.build=${BUILD}" github.com/coulterac/kafka/cmd/consumer@latest
RUN go install -ldflags "-X main.build=${BUILD}" github.com/coulterac/kafka/cmd/producer@latest