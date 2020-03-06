#!/bin/sh
echo ""
echo "################################################"
SCAN_FILE=$*
echo $(date "+%Y-%m-%d %H:%M:%S") "start scan file: "$SCAN_FILE
/usr/bin/clamdscan --fdpass $SCAN_FILE
EXIT_CODE=$?
echo $(date "+%Y-%m-%d %H:%M:%S") "clamdscan return code is: "$EXIT_CODE
echo "################################################"
echo ""
exit $EXIT_CODE
