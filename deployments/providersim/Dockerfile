FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY main.go ./
COPY templates ./templates
COPY static ./static
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/providersim .

FROM alpine:3.21
COPY --from=build /out/providersim /usr/local/bin/providersim
USER nobody
EXPOSE 8090
ENTRYPOINT ["providersim"]
