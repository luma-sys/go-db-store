# Running Tests with Docker

This guide explains how to run `go-db-store` unit tests using Docker.

## 📋 Prerequisites

- Docker installed
- Docker Compose (optional, but recommended)

## 🚀 Execution Methods

### Method 1: Helper Script (Recommended)

The simplest method is to use the `test-docker.sh` script:

```bash
# Run all tests
./test-docker.sh

# Run specific tests
./test-docker.sh go test -v ./store -run TestMongoSave

# Run only MongoDB tests
./test-docker.sh go test -v ./store -run "^TestMongo"
```

### Method 2: Docker Compose

```bash
# Run all tests
docker-compose -f docker-compose.test.yml up --build

# Run and remove containers after
docker-compose -f docker-compose.test.yml up --build --abort-on-container-exit

# Build only
docker-compose -f docker-compose.test.yml build
```

### Method 3: Direct Docker

```bash
# Build the image
docker build -f Dockerfile.test -t go-db-store-test .

# Run all tests
docker run --rm go-db-store-test

# Run specific tests
docker run --rm go-db-store-test go test -v ./store -run TestMongoSave

# Run with mounted volume (for development)
docker run --rm -v $(pwd):/app go-db-store-test
```

## 🔧 Advanced Configuration

### Environment Variables

You can override the MongoDB URL using environment variables:

```bash
# With script
MONGODB_DOWNLOAD_URL="https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2404-7.0.14.tgz" ./test-docker.sh

# With docker-compose
MONGODB_DOWNLOAD_URL="https://..." docker-compose -f docker-compose.test.yml up

# With direct docker
docker run --rm \
  -e MONGODB_DOWNLOAD_URL="https://..." \
  go-db-store-test
```

### Running with Module Cache

To improve performance across multiple runs:

```bash
docker run --rm \
  -v $(pwd):/app \
  -v go-modules-cache:/go/pkg/mod \
  go-db-store-test
```

## 🐛 Debugging

### Enter the Container

```bash
# Start an interactive shell
docker run --rm -it \
  -v $(pwd):/app \
  go-db-store-test \
  /bin/bash

# Inside the container you can run:
go test -v ./store
go test -v ./store -run TestMongoSave
```

### View Detailed Logs

```bash
# Increase timeout and verbosity
docker run --rm go-db-store-test \
  go test -v ./... -timeout 600s -count=1
```

## 📊 CI/CD

### GitHub Actions

```yaml
- name: Run tests in Docker
  run: |
    docker build -f Dockerfile.test -t go-db-store-test .
    docker run --rm go-db-store-test
```

### GitLab CI

```yaml
test:
  image: docker:latest
  services:
    - docker:dind
  script:
    - docker build -f Dockerfile.test -t go-db-store-test .
    - docker run --rm go-db-store-test
```

## 🏗️ Files

- `Dockerfile.test` - Docker image based on `golang:1.25`
- `docker-compose.test.yml` - Docker Compose configuration
- `test-docker.sh` - Helper script for simplified execution
- `.dockerignore` - Files ignored during build

## 💡 Tips

1. **Dependency Cache**: The Dockerfile is optimized to cache Go modules
2. **Multi-stage**: For production, consider using multi-stage builds
3. **Volumes**: Mount code as volume during development for faster tests
4. **Cleanup**: Use `--rm` to automatically remove containers after execution

## ❓ Common Issues

### MongoDB doesn't start

If tests fail with MongoDB timeout:

1. Increase timeout: `docker run --rm go-db-store-test go test -v ./... -timeout 600s`
2. Check MongoDB version in `mongo_test.go`
3. Configure `MONGODB_DOWNLOAD_URL` for your distribution

### Permission denied

```bash
chmod +x test-docker.sh
```

### Stale cache

```bash
docker-compose -f docker-compose.test.yml build --no-cache
# or
docker build -f Dockerfile.test -t go-db-store-test --no-cache .
```