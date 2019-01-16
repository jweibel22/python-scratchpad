#!/bin/bash
idx=0
message_template=`cat message.json`

while true
do
    message=${message_template/11233232/$idx}
    curl -H "Content-Type: application/json" -H "Data-Platform-Gateway-Version: 2018-11-12" -X POST http://localhost:8080/events/test.commit_status -d "${message}"
    echo "Message published, press [CTRL+C] to stop.."
	sleep 1
    idx=$((idx + 1))
done
