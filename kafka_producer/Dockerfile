FROM python:3.9-slim 

RUN pip3 install kafka-python==2.0.2
RUN pip3 install avro==1.11.1
RUN pip3 install pandas==1.5.1
RUN pip3 install pyarrow==10.0.1
RUN pip3 install python-schema-registry-client==2.4.1
RUN pip3 install pymongo==4.5.0

COPY kafka_producer/produce_data.py produce.py 

CMD ["python", "-u", "produce.py", "--mode", "setup", "--bootstrap_servers", "broker:29092"]
