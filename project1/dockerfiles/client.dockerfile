FROM eafdeafd/kvs:base

USER root

WORKDIR $KVS_HOME

CMD python3 client.py -i $CLIENT_ID
