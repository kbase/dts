FROM --platform=linux/amd64 ubuntu:latest

ENV DEBIAN_FRONTEND=noninteractive

# Install iRODS dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    lsb-release \
    postgresql-client \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Install iRODS
RUN wget -qO - https://packages.irods.org/irods-signing-key.asc | apt-key add - && \
    echo "deb [arch=amd64] https://packages.irods.org/apt/ $(lsb_release -sc) main" | tee /etc/apt/sources.list.d/renci-irods.list && \
    apt-get update && \
    apt-get install -y \
    irods-server \
    irods-database-plugin-postgres \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 1247 1248 20000-20199

# Startup script
COPY provider-config.json /provider-config.json
COPY start.sh /start.sh
RUN chmod +x /start.sh
CMD ["/start.sh"]