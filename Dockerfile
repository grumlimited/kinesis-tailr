FROM library/rust:1.69.0-slim-buster

RUN apt-get update && \
	apt-get install -y make sudo


WORKDIR /kinesis-tailr

ENTRYPOINT ["make", "debian-pkg", "RELEASE_VERSION=0.1", "DESTDIR=build"]

