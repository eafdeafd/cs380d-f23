FROM eafdeafd/kvs:base

USER root

WORKDIR $KVS_HOME

CMD python3 frontend.py
