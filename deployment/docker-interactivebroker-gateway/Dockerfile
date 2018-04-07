FROM consol/ubuntu-xfce-vnc

MAINTAINER Christophe <chris.perso@gmail.com>

ENV REFRESHED_AT 2018-03-18

USER 0

RUN \
    apt-get update -y \
    && apt-get install -y software-properties-common python-software-properties unzip

# Java 8
RUN \
    echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections \
    && add-apt-repository -y ppa:webupd8team/java \
    && apt-get update \
    && apt-get install -y oracle-java8-installer \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/oracle-jdk8-installer \
    && apt-get -y autoremove \
    && apt-get clean

# IBroker Gateway
WORKDIR /tmp
ENV INSTALL4J_ADD_VM_PARAMS="-Djava.awt.headless=true"
RUN wget https://download2.interactivebrokers.com/installers/ibgateway/stable-standalone/ibgateway-stable-standalone-linux-x64.sh && \
    chmod a+x *.sh && \
    echo "n" | ./ibgateway-stable-standalone-linux-x64.sh && \
    rm -rf /tmp/*

# IBController
RUN mkdir -p /opt/IBController \
    && wget https://github.com/ib-controller/ib-controller/releases/download/3.4.0/IBController-3.4.0.zip \
    && unzip /tmp/IBController-3.4.0.zip -d /opt/IBController \
    && chmod a+x /opt/IBController/*.sh


#    sed -i 's/CONF_CONTROLLER_PORT/'"$CONF_CONTROLLER_PORT"'/g' $BASE_DIR/IBController/IBController.ini
#    sed -i 's/CONF_API_PORT/'"$CONF_API_PORT"'/g' $BASE_DIR/IBController/IBController.ini
#    sed -i 's/CONF_TRADING_MODE/'"$CONF_TRADING_MODE"'/g' $BASE_DIR/IBController/IBController.ini
#    sed -i 's/CONF_IB_USER/'"$CONF_IB_USER"'/g' $BASE_DIR/IBController/IBController.ini
#    sed -i 's/CONF_IB_PASS/'"$CONF_IB_PASS"'/g' $BASE_DIR/IBController/IBController.ini
#    sed -i 's/CONF_IB_READ_ONLY/'"$CONF_IB_READ_ONLY"'/g' $BASE_DIR/IBController/IBController.ini

#COPY config/IBController.ini /root/IBController/IBController.ini
#COPY config/jts.ini /opt/IBJts/jts.ini
#COPY init/xvfb_init /etc/init.d/xvfb
#COPY init/vnc_init /etc/init.d/vnc
#COPY bin/xvfb-daemon-run /usr/bin/xvfb-daemon-run
#COPY bin/run-gateway /usr/bin/run-gateway

#USER 1000

# install init scripts and binaries
#ADD config/jts.ini /ib-gateway/jts.ini
#ADD init/xvfb_init /etc/init.d/xvfb
#ADD init/vnc_init /etc/init.d/vnc
#ADD bin/xvfb-daemon-run /usr/bin/xvfb-daemon-run
#ADD bin/run-gateway /usr/bin/run-gateway

# 5900 for VNC, 4003 for the gateway API via socat
#EXPOSE 4003
#VOLUME /opt/ibgateway

#CMD ["/usr/bin/run-gateway"]
