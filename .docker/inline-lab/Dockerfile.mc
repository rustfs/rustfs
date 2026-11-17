FROM debian:trixie-slim
COPY mc /usr/bin/mc
RUN chmod 0755 /usr/bin/mc && /usr/bin/mc --version
LABEL rustfs.validation="inline"
ENTRYPOINT ["/usr/bin/mc"]
