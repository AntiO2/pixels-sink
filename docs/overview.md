# Pixels-Sink Overview


![](./assets/frame.png)


Pixels Sink目前可以简单分为多层流水线架构，每层之间通过生产者/消费者模式传递数据。

## Entry

介绍Pixels-Sink启动的大致流程

### PixelsSinkApp

目前启动的主类。

通过`-c ${filePath}` 传递一个properties文件，该文件会和`${PIXELS_HOME}`（或者pixels默认配置） 下的`pixels.properties`配置文件组合，构成Pixels Sink的配置。

配置类位于`PixelsSinkConfig`，目前支持`Integer`、`Long`、`Short`、`Boolean`类型配置的自动解析。

### PixelsSinkProvider

实现了Pixels的SPI, 作为Pixles-Worker启动(没测过)。

## Source

Source层从数据源拉取数据，并进行解析。

### Source Input

| Source Type | 说明                                                                                 | 相关配置项 |
| ----------- | ---------------------------------------------------------------------------------- | ----- |
| engine      | 使用Debezium Engine，直接接到数据库上读取WAL/binlog等                                            |       |
| kafka       | 从kafka读取数据                                                                         |       |
| storage     | 利用pixels-storage读取数据。数据格式为sink.proto序列化之后的二进制文件，格式为keyLen + key + valueLen + value |       |

### Source Output

在Source层，不会做解析操作，而是直接将二进制数据传递给对应的Provider

## Provider


Provider层

```mermaid
classDiagram
    direction TB

    class EventProvider~SOURCE_RECORD_T, TARGET_RECORD_T~ {
        +run()
        +close()
        +processLoop()
        +convertToTargetRecord()
        +recordSerdEvent()
        +putRawEvent()
        +getRawEvent()
        +pollRawEvent()
        +putTargetEvent()
        +getTargetEvent()
    }

    class TableEventProvider~SOURCE_RECORD_T~ {
    }
    class TableEventEngineProvider~T~ {
    }
    class TableEventKafkaProvider~T~ {
    }
    class TableEventStorageProvider~T~ {
    }

    class TransactionEventProvider~SOURCE_RECORD_T~ {
    }
    class TransactionEventEngineProvider~T~ {
    }
    class TransactionEventKafkaProvider~T~ {
    }
    class TransactionEventStorageProvider~T~ {
    }

    EventProvider <|-- TableEventProvider
    EventProvider <|-- TransactionEventProvider

    TableEventProvider <|-- TableEventEngineProvider
    TableEventProvider <|-- TableEventKafkaProvider
    TableEventProvider <|-- TableEventStorageProvider

    TransactionEventProvider <|-- TransactionEventEngineProvider
    TransactionEventProvider <|-- TransactionEventKafkaProvider
    TransactionEventProvider <|-- TransactionEventStorageProvider

```

Provider层通过实现`EventProvider<SOURCE_RECORD_T, TARGET_RECORD_T>` ,将Source_Record_T 转化为Target_Record_T。

例如

| Provider                        | Source Type     | Target Type                   |
| ------------------------------- | --------------- | ----------------------------- |
| TableEventEngineProvider        | Debezium Struct | RowChangeEvent                |
| TableEventKafkaProvider         | kafka topic     | RowChangeEvent                |
| TableEventStorageProvider       | Proto 二进制数据     | RowChangeEvent                |
|                                 |                 |                               |
| TransactionEventEngineProvider  | Debezium Struct | SinkProto.TransactionMetadata |
| TransactionEventKafkaProvider   | kafka topic     | SinkProto.TransactionMetadata |
| TransactionEventStorageProvider | Proto 二进制数据     | SinkProto.TransactionMetadata |


## Processor

Processor从Provider中拉取数据，写到对应的Retina Writer中。

TableProcessor被`TableProviderAndProcessorPipelineManager` 创建，通常每个表对应一个Processor。确保同一个表上记录的顺序。

TransactionProcessor只有一个实例。

## Writer

Writer需要实现以下接口

| 接口                                                            | 说明      |
| ------------------------------------------------------------- | ------- |
| writeRow(RowChangeEvent rowChangeEvent)                       | 写行变更    |
| writeTrans(SinkProto.TransactionMetadata transactionMetadata) | 提供事务元信息 |
| flush()                                                       | 刷数据     |

### Retina Writer

RetinaWriter类实现了PixelsSinkWriter的接口。实现了支持事务的数据回放。

#### RetinaServiceProxy

用于和Retina通信的客户端，持有一个RetinaService。

#### Context Manager

单例，持有所有事务的上下文和TableWriterProxy。

决定了什么时候开启事务、什么时候刷数据、怎么提交事务。

**分桶**
在处理RowChangeEvent时，做如下处理： ^2906a9
- **Insert**
    - 根据AfterData的Key获取对应Bucket，得到对应TableWriter并写入
- **Delete**
    - 根据BeforeData的Key获取对应Bucket，得到对应TableWriter并写入
- **Update**
    - 如果主键未发生变更，则任意取一个Key，得到对应的Bucket和TableWriter并写入。
    - 如果主键发生了变更，则构造对应的Delete和Insert的RowChangeEvent，这两个Event可能由不同Bucket的TableWriter写入，也可能是同一个Bucket，遵守先Delete再Insert的写入次序。

![img.png](./assets/TransactionCoordinator.png)

#### Table Writer

每个Table Writer持有多个RetinaClient

有两个实现：
1. SingleTxWriter, 每次写同一个事务。
2. CrossTxWriter, 每次发送RPC可能是多个事务。

TableWriter写入行变更，（对于Stub，是同步的；对于Stream，在Observer回调中处理）更新对应的SinkContext计数器，如果写入失败，则Rollback事务。

#### TransactionProxy

持有一个TransactionService，提供了同步或异步提交事务的接口。

异步提交事务时，TransactionProxy线程在后台进行批量提交。




### Proto Writer

用来造storage source。将数据按照顺序序列化成proto格式。元信息（文件路径等）存在ETCD里

### CSV Writer


### Flink Writer

