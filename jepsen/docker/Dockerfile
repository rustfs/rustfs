FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openssh-server \
        sudo \
        ca-certificates \
        curl \
        wget \
        unzip \
        iptables \
        iproute2 \
        procps \
        netcat-openbsd \
        coreutils \
        findutils \
        iputils-ping \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/bash jepsen \
    && printf 'jepsen ALL=(ALL) NOPASSWD:ALL\nDefaults:jepsen !requiretty\n' > /etc/sudoers.d/jepsen \
    && chmod 440 /etc/sudoers.d/jepsen \
    && mkdir -p /run/sshd /home/jepsen/.ssh \
    && chown -R jepsen:jepsen /home/jepsen/.ssh \
    && chmod 700 /home/jepsen/.ssh

COPY entrypoint.sh /usr/local/bin/jepsen-entrypoint
RUN chmod 755 /usr/local/bin/jepsen-entrypoint

EXPOSE 22 9000
ENTRYPOINT ["/usr/local/bin/jepsen-entrypoint"]
