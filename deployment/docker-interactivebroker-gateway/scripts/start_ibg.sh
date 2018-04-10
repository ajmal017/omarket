#!/usr/bin/env bash

echo "Using display $DISPLAY"
echo "Launching IBroker Gateway"
$IBC_PATH/IBControllerGatewayStart.sh
echo "IBroker Gateway starting in background..."
echo "Forking 127.0.0.1:4002 onto 0.0.0.0:4003"
socat TCP-LISTEN:4003,fork TCP:127.0.0.1:4002
