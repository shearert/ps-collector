

FROM python:3-alpine

RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*

ADD . /ps_collector
WORKDIR /ps_collector
RUN pip install -r requirements.txt

RUN python setup.py install

EXPOSE 8000

CMD ps-collector
