FROM golang:1.19-alphine as backend
RUN apk add --update  --no-cache bash ca-certificates curl git make tzdata

RUN mkdir -p /go/src/scheduler
WORKDIR /go/src/scheduler
ADD . /go/src/scheduler

RUN go build -o scheduler main.go

FROM alphine:3.17
COPY --from=backend /usr/share/zoneinfo /usr/share/zoneinfo/
COPY --from=backend /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=backend /go/src/scheduler/scheduler /bin

ENTRYPOINT ["/bin/scheduler"]