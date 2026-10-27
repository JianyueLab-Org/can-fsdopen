FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY go.mod go.sum README.md config.example.json LICENSE THIRD_PARTY_NOTICES.md .gitignore .dockerignore Dockerfile ./
COPY auth/*.go ./auth/
COPY config/*.go ./config/
COPY fsd/*.go ./fsd/
COPY cmd/fsdopen/*.go ./cmd/fsdopen/
COPY tools/release.sh ./tools/release.sh
RUN sh tools/release.sh /out

FROM gcr.io/distroless/static-debian12:nonroot
LABEL org.opencontainers.image.licenses="AGPL-3.0-only" \
      org.opencontainers.image.source="file:///app/source.tar.gz" \
      org.opencontainers.image.description="Corresponding Source: /app/source.tar.gz; build instructions: /app/SOURCE.txt"
WORKDIR /app
COPY --from=build /out/ /app/
EXPOSE 6809 6820
ENTRYPOINT ["/app/fsdopen"]
CMD ["-config", "/config/config.json"]
