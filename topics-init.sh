#!/bin/bash
# echo " topics-init.sh is running!" >> /tmp/debug.log
echo "🔹 Creating Topics in Kafka..."
/usr/bin/kafka-topics --create --topic eventsim-input --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
/usr/bin/kafka-topics --create --topic spark-output --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
echo "✅ Topics created successfully!"