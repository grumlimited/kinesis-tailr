FROM amd64/centos:7

ARG USER=grumlimited
ARG GROUP=grumlimited
ARG UID=1000
ARG GID=1000

ENV HOME=/home/$USER

# https://stackoverflow.com/questions/74345206/centos-7-docker-yum-installation-gets-stuck
RUN echo 'hosts: files mdns4_minimal [NOTFOUND=return] dns mdns4' > /etc/nsswitch.conf

RUN groupadd -g $GID $USER && \
 	useradd $USER -d $HOME -g $GID -u $UID && \
 	chown -R $USER:$GROUP $HOME


RUN ulimit -n 1024 && yum install -y epel-release
RUN ulimit -n 1024 && yum install -y epel-release && \
	yum update -y && \
	yum install -y rpm-build rust make cargo

USER $USER
