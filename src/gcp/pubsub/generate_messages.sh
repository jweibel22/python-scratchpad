#!/bin/bash

while true
do
    gcloud pubsub topics publish $1 --message "hello" > /dev/null
    echo "Message published, press [CTRL+C] to stop.."
	sleep 1
done
