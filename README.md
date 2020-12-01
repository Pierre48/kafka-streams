# Development 

## Create your topics
''''
docker exec -it  broker  /bin/sh
kafka-topics -create --topic test --bootstrap-server localhost:9092
kafka-topics -create --topic test-car --bootstrap-server localhost:9092
                                C
kafka-topics -list --bootstrap-server localhost:9092
''''

docker-compose build
docker-compose up -d


