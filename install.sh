#!/usr/bin/env bash

VAR_DIR="/var/.sige/"
ETC_DIR="/etc/sige/"
VAR_LOG_DIR="/var/log/sige/"

if [ $# != 1 ]; then
    echo "Usage: ./install.sh ENV_TYPE"
    echo "where:"
    echo "  ENV_TYPE    PROD for production or TEST for Test environment."
    exit 1

fi

if [ $EUID -ne 0 ]; then
    echo "You must be a root user!!" 2>&1
    exit 1
fi

if [ `cat /etc/passwd | egrep "^sige:" | wc -c` -eq 0 ]; then
    adduser --gecos "" --disabled-password --no-create-home -q sige
fi
[ -d $VAR_DIR ] || mkdir $VAR_DIR
[ -d $ETC_DIR ] || mkdir $ETC_DIR
[ -d $VAR_LOG_DIR ] || mkdir $VAR_LOG_DIR
echo "PRODUCTION=`test \"$1\" == \"PROD\" && echo 1 || echo 0`" >> "$VAR_DIR/.environment"
echo "### Instaled at: `date +\"%d/%m/%Y %H:%M:%S\"` ###" >> "$VAR_LOG_DIR/sige_core.log"
echo "### Instaled at: `date +\"%d/%m/%Y %H:%M:%S\"` ###" >> "$VAR_LOG_DIR/sige_web.log"
echo "### Instaled at: `date +\"%d/%m/%Y %H:%M:%S\"` ###" >> "$VAR_LOG_DIR/sige_web_service.log"
cp -r ../* $ETC_DIR
chown -R sige:sige $VAR_DIR
chown -R sige:sige $VAR_LOG_DIR
chown -R root:sige $ETC_DIR