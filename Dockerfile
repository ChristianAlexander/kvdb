FROM golang as build

WORKDIR /kvdb

COPY . .

RUN CGO_ENABLED=0 go build -o /bin/kvapi cmd/kvapi/*.go

FROM scratch

COPY --from=build /bin/kvapi /kvapi

EXPOSE 3001
ENTRYPOINT ["/kvapi"]
