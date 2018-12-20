

FROM python:2-alpine3.7

ADD . /ps_collector
WORKDIR /ps_collector
RUN pip install -r requirements.txt

RUN python setup.py install

CMD ps-collector
