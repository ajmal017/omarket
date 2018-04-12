FROM consol/ubuntu-xfce-vnc

MAINTAINER Christophe <chris.perso@gmail.com>

ENV REFRESHED_AT 2018-03-18

USER 0

RUN groupadd -r ibguser \
    && useradd -m -d /headless -r -u 1000 -g ibguser ibguser \
    && chown -R ibguser:ibguser /headless

# Some required tools
RUN \
    apt-get update -y \
    && apt-get install -y software-properties-common python-software-properties unzip xterm socat git-core

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

WORKDIR /tmp

# IBController
RUN mkdir -p /opt/IBController \
    && wget https://github.com/ib-controller/ib-controller/releases/download/3.4.0/IBController-3.4.0.zip \
    && unzip /tmp/IBController-3.4.0.zip -d /opt/IBController \
    && chmod a+x /opt/IBController/*.sh \
    && chmod a+x /opt/IBController/Scripts/*.sh \
    && rm -f /tmp/IBController-3.4.0.zip \
    && mkdir -p /headless/IBController/Logs \
    && chown -R ibguser:ibguser /headless/IBController

# Installing Python IBroker libs for testing purposes
RUN cd /tmp \
    && git clone https://github.com/blampe/IbPy \
    && cd /tmp/IbPy \
    && python setup.py install

ADD config/IBController.ini /headless/IBController/IBController.ini
ADD config/jts.ini          /headless/Jts/jts.ini
ADD scripts/start_ibg.sh    /headless/start_ibg.sh
RUN chown -R ibguser:ibguser /headless \
    && chmod 755 /headless/*.sh

USER ibguser
WORKDIR /headless

# IBroker Gateway
RUN wget https://download2.interactivebrokers.com/installers/ibgateway/stable-standalone/ibgateway-stable-standalone-linux-x64.sh
ENV INSTALL4J_ADD_VM_PARAMS="-Djava.awt.headless=true"
ENV CONF_TRADING_MODE="paper"
ENV CONF_IB_USER="edemo"
ENV CONF_IB_PASS="demouser"
RUN chmod a+x *.sh \
    && echo "n" | ./ibgateway-stable-standalone-linux-x64.sh \
    && rm -f /headless/ibgateway-stable-standalone-linux-x64.sh \
    && echo "\n-Djava.awt.headless=false\n" >> /headless/Jts/ibgateway/963/ibgateway.vmoptions \
    && sed -i 's/%CONF_TRADING_MODE%/'"$CONF_TRADING_MODE"'/g' /headless/IBController/IBController.ini \
    && sed -i 's/%CONF_IB_USER%/'"$CONF_IB_USER"'/g' /headless/IBController/IBController.ini \
    && sed -i 's/%CONF_IB_PASS%/'"$CONF_IB_PASS"'/g' /headless/IBController/IBController.ini

ENV IBC_PATH=/opt/IBController

EXPOSE 4003

ENTRYPOINT ["/dockerstartup/vnc_startup.sh"]
CMD ["/headless/start_ibg.sh"]

