FROM centos:7
MAINTAINER mfh
RUN mkdir /usr/local/go
WORKDIR /usr/local/go

ADD main /usr/local/go


RUN chmod 777 /usr/local/go/main
