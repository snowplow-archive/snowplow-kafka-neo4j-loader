# Kafka to Neo4j simple scala storage loadet

## Getting Started

This Code is ready to use Storage loader which save page url , referer url , derived time ,and session id. By these fields, we are able to make [WENT_TO] relation that shows how site visitor search through webpages of our site.


### Configuration

In the config.conf file you can adjust your Ne04j storage loader with cofiguration that you want.

Firstly, Set you Kafka connection information in below. It is better to set specific consumer id in order to Kafka manages offsets. you can seperate yor brokers just by comma !

```
kafka{
  brokers = "broker1,broker2"
  topic = "topicName"
  consumerGroupId = "consumerGroupID"
}
```

Secondly, You should set you Neo4j Connection information. In numberOfThread field you should mention that how many thread you need to start.

```
neo4j{
  url = "bolt://localhost:7687"
  username = "########"
  password = "##########"
  groupid = "###########"
  numberOfThreads = 1
}
```

## Contributing

This Program is Coded by [Digikala](https://Digikala.com) Data Science Team.

## RoadMap

we will add *add to cart* and *add transaction* events as soon as possible. Furthermore, Page Category will be added to this project to aggregate page visits.

## Author

* **SeyedFarzam Mirmoeini** - *Data Science Specialist* - [Github](https://github.com/mirfarzam)

