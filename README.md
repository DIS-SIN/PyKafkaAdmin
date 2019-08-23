# PyKafkaAdmin

Simple REST Endpoint for Kafka 


## Context

Kafka REST proxy is stateful and uses non standard media types. This combined with the number of requests needed to set up a producer and consumer means it is unsutable for front end development. I have set up a simplified API to allow messages to be consumed and produced easily in one request


## API

/topics

*Description*: Returns a list of all the topics in kafka

*METHOD*: GET

*Example Response*:

```JSON
[
"_confluent-controlcenter-5-1-2-1-group-stream-extension-rekey",
"_confluent-controlcenter-5-1-2-1-aggregatedTopicPartitionTableWindows-THREE_HOURS-changelog",
"_confluent-controlcenter-5-1-2-1-Group-THREE_HOURS-changelog",
"_confluent-controlcenter-5-1-2-1-TriggerActionsStore-changelog",
"_confluent-controlcenter-5-1-2-1-error-topic",
"docker-connect-status",
"_confluent-controlcenter-5-1-2-1-group-aggregate-topic-ONE_MINUTE-changelog",
"_schemas",
"_confluent-controlcenter-5-1-2-1-Group-ONE_MINUTE-changelog",
"_confluent-controlcenter-5-1-2-1-KSTREAM-OUTERTHIS-0000000095-store-changelog",
"text_data_to_be_processed",
"_confluent-controlcenter-5-1-2-1-MonitoringVerifierStore-changelog",
"_confluent-controlcenter-5-1-2-1-actual-group-consumption-rekey",
"_confluent-controlcenter-5-1-2-1-group-aggregate-topic-ONE_MINUTE",
"_confluent-controlcenter-5-1-2-1-monitoring-aggregate-rekey",
"_confluent-monitoring",
"_confluent-controlcenter-5-1-2-1-MetricsAggregateStore-repartition",
"text_data_to_be_processed_2",
"_confluent-controlcenter-5-1-2-1-KSTREAM-OUTEROTHER-0000000096-store-changelog",
"_confluent-controlcenter-5-1-2-1-MonitoringMessageAggregatorWindows-ONE_MINUTE-changelog",
"text_data_processed",
"_confluent-controlcenter-5-1-2-1-monitoring-trigger-event-rekey",
"_confluent-controlcenter-5-1-2-1-MonitoringMessageAggregatorWindows-THREE_HOURS-changelog",
"__consumer_offsets",
"_confluent-controlcenter-5-1-2-1-MonitoringTriggerStore-changelog",
"_confluent-controlcenter-5-1-2-1-metrics-trigger-measurement-rekey",
"_confluent-controlcenter-5-1-2-1-group-aggregate-topic-THREE_HOURS",
"_confluent-controlcenter-5-1-2-1-MonitoringStream-THREE_HOURS-changelog",
]

```

/topics/&lt;string:topic>

*Description*: Tells you whether or not a topic exists

*Example Responses*

#### Topic Exists 
status: 201

""

#### Topic DNE
status 404
```JSON 
{
    "message": "Topic '<topic>' does not exist"
}
```

/produce

*Description*: Produce a message to a topic using an avro schema

*METHOD*: POST

*Example Request*

```JSON

{
	"topic": "TestTopic",
	"avro_schema": {
		"namespace": "test.something",
		"type": "record",
		"name": "test",
		"fields": [
			{"name": "hello", "type": "string"}
		]
	},
	"message":{
		"hello": "woresdgfdsgrdfweefewfseld"
	}
}
```

*Example Response*

status: 201 

""

/topic/&lt;string:topic>/produce

*METHOD*: POST

*Example Request*

```JSON

{
	"avro_schema": {
		"namespace": "test.something",
		"type": "record",
		"name": "test",
		"fields": [
			{"name": "hello", "type": "string"}
		]
	},
	"message":{
		"hello": "woresdgfdsgrdfweefewfseld"
	}
}
```

*Example Response*

status: 201 

""

/consume


*METHOD*: POST

*Example Request*

```JSON
{
	"topic": "TestTopic"
}
```

*Example Response*
```JSON
{
    "hello": "woresdgfdsgrdfweefewfseld"
}
```

/topic/&lt;string:topic>/consume

*METHOD*: GET

*EXAMPLE_RESPONSE*: Same As Above






