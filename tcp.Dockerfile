FROM golang as build

WORKDIR /kvdb

COPY . .

RUN CGO_ENABLED=0 go build -o /bin/kv-tcp cmd/kv-tcp/*.go

FROM scratch

COPY --from=build /bin/kv-tcp /kv-tcp

EXPOSE 8888
ENTRYPOINT ["/kv-tcp"]
