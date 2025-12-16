FROM apache/airflow:2.9.3

USER root

# 1. Instalar dependencias de 32 bits necesarias para SteamCMD
RUN dpkg --add-architecture i386 && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    lib32gcc-s1 \
    lib32stdc++6 \
    curl \
    ca-certificates \
    tar \
    && rm -rf /var/lib/apt/lists/*

# 2. Instalar SteamCMD en /steamcmd
RUN mkdir -p /steamcmd && \
    curl -sqL "https://steamcdn-a.akamaihd.net/client/installer/steamcmd_linux.tar.gz" | tar zxvf - -C /steamcmd && \
    chown -R airflow:root /steamcmd

# Volver al usuario airflow
USER airflow