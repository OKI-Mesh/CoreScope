# Linux dev environment matching the CI runner (ubuntu, bash 5, Node 22, Go 1.26).
# Keep the versions in step with .github/workflows/deploy.yml (node-version, GO_VERSION).
FROM mcr.microsoft.com/devcontainers/base:ubuntu-24.04

# psql: used when building the E2E database and by the E2E scripts. fuser (psmisc) frees the test port.
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client psmisc \
    && rm -rf /var/lib/apt/lists/*
 
ARG NODE_MAJOR=22
ARG GO_VERSION=1.26.0
 
RUN curl -fsSL https://deb.nodesource.com/setup_${NODE_MAJOR}.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*
 
RUN ARCH="$(dpkg --print-architecture)" \
    && curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${ARCH}.tar.gz" | tar -C /usr/local -xz
ENV PATH="/usr/local/go/bin:${PATH}"
