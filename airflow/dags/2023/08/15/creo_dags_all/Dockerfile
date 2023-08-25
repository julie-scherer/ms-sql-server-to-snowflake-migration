FROM --platform=linux/amd64 quay.io/astronomer/astro-runtime:6.0.3-base AS stage1

## Set labels
LABEL maintainer="Astronomer <humans@astronomer.io>"
ARG BUILD_NUMBER=-1
LABEL io.astronomer.docker=true
LABEL io.astronomer.docker.build.number=$BUILD_NUMBER
LABEL io.astronomer.docker.airflow.onbuild=true

## Install packages in packages.txt file if any exist
USER root
COPY packages.txt .
RUN if [[ -s packages.txt ]]; then \
    apt-get update && cat packages.txt | tr '\r\n' '\n' | sed -e 's/#.*//' | xargs apt-get install -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*; \
  fi

## Add CA certificate to the Astro runtime image
COPY ./certs /usr/local/share/ca-certificates/curo
RUN if [ "$(ls -f /usr/local/share/ca-certificates/curo | grep -E '\.(pem|crt|cert)$')" ]; then \
		echo "Found CA certificates: $(ls -f /usr/local/share/ca-certificates/curo | grep -E '\.(pem|crt|cert)$')" \
		&& apt-get update && apt-get install -y \
			ca-certificates \
			curl \
		&& rm -rf /var/lib/apt/lists/* \
		&& cp /usr/local/share/ca-certificates/curo/*.pem /usr/local/share/ca-certificates/curo/certificates.crt \
		&& cp /usr/local/share/ca-certificates/curo/*.pem /etc/ssl/certs/certificates.crt \
		&& update-ca-certificates; \
	else \
		echo "No CA certificates found" \
		&& cp /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/certificates.crt; \
	fi
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/certificates.crt


# ** - - - - - - - - - - - - - - - **
# apt-get and system utilities
RUN apt-get update && apt-get install -y \
    	curl apt-utils apt-transport-https debconf-utils gcc build-essential wget gnupg2 \
    && rm -rf /var/lib/apt/lists/*

# Install SQL Server drivers and tools
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && ACCEPT_EULA=Y apt-get install -y mssql-tools \
    && apt-get install -y unixodbc-dev \
    && apt-get install -y pigz \
    && rm -rf /var/lib/apt/lists/*
# ** - - - - - - - - - - - - - - - **


## Install Python packages
FROM stage1 AS stage2
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

## Copy requirements directory
FROM stage1 AS stage3
COPY --from=stage2 /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
COPY --from=stage2 /usr/local/bin /home/astro/.local/bin
ENV PATH="/opt/mssql-tools/bin:$PATH"
ENV PATH="/home/astro/.local/bin:$PATH" 

## Copy entire project directory
COPY --chown=astro:0 . .

## Enable XCOM pickling in Airflow
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True