FROM python:3.7-slim
MAINTAINER yelo.blood <yelo.blood@kakaopaycorp.com>
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN ln -s /usr/bin/pip3 /usr/bin/pip

COPY . ~/s3toredis
WORKDIR ~/s3toredis
RUN pip install -r requirements.txt

CMD ["python","~/s3toredis/s3toredis.py"]
