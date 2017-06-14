# Orleans.Providers.RabbitMQ
RabbitMQ persistent stream provider for Microsoft Orleans

## Overview
The RabbitMQStreamProvider is a new implementation of a PersistentStreamProvider for Microsoft Orleans. 

## Implementation
The RabbitMQStreamProvider is implemented using the Orleans Guidelines to implement a new PersistentStreamProvider over the PersistentStreamProvider class (shown in this page: http://dotnet.github.io/orleans/Orleans-Streams/Streams-Extensibility)

###The main classes written were:
- RabbitMQAdapterFactory
- RabbitMQAdapter
- RabbitMQAdapaterReceiver
- RabbitMQBatchContainer
- RabbitMQStreamProviderConfiguration
- RabbitMQResourceManager
- RabbitMQDataAdapter

## <a name="configurableValues"></a>Configurable Values
These are the configurable values that the RabbitMQStreamProvider offers to its users, the first two are required while the others have default values:

- **DataConnectionString**: rabbitmq connection string, it needs to be valid AMQP URI string.
- **DeploymentId**: The deployement ID.
- **NumQueues**: The number of virtual queues, default is 1.
- **Exchange**: The exchange name which will be used to publish the messages.
- **ExchangeType**: The exchange type, default is Direct.
- **ExchangeDurable**: The exhange Durability settings, default is true.
- **AutoDelete**: Auto delete queues and exchanges, default is false. 
- **Queue**: The Queue Name
- **QueueDurable**: Queue Durability, default is true.
- **RoutingKey**: The routing key 


