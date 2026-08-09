# -------- Build stage --------
FROM golang:1.25.11-bookworm AS build
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/orchestration-service .

# -------- Runtime stage --------
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app

COPY --from=build /app/orchestration-service /app/orchestration-service

EXPOSE 8082

ENTRYPOINT ["/app/orchestration-service"]
