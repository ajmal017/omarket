#!/usr/bin/env bash

echo "Using display $DISPLAY"

echo "---------------- Preparing IBroker Gateway config file"
echo "-Djava.awt.headless=false" >> /headless/Jts/ibgateway/963/ibgateway.vmoptions
sed -i 's/%CONF_TRADING_MODE%/'"$CONF_TRADING_MODE"'/g' /headless/IBController/IBController.ini
sed -i 's/%CONF_IB_USER%/'"$CONF_IB_USER"'/g' /headless/IBController/IBController.ini
sed -i 's/%CONF_IB_PASS%/'"$CONF_IB_PASS"'/g' /headless/IBController/IBController.ini

echo "---------------- Launching IBroker Gateway"
$IBC_PATH/IBControllerGatewayStart.sh
echo "IBroker Gateway starting in background..."
echo "Forking 127.0.0.1:4002 onto 0.0.0.0:4003"
socat TCP-LISTEN:4003,fork TCP:127.0.0.1:4002
