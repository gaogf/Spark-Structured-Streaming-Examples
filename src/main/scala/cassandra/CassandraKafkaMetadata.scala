package cassandra

import kafka.KafkaMetadata

object CassandraKafkaMetadata {
  private def cql(metadata: KafkaMetadata): String = s"""
       INSERT INTO test.kafkametadata (partition, offset)
       VALUES(${metadata.partition}, ${metadata.offset})
    """

  //https://github.com/datastax/spark-cassandra-connector/blob/master/doc/1_connecting.md#connection-pooling
  //保存元素数据
  def save(metadata: KafkaMetadata) = {
    CassandraDriver.connector.withSessionDo(session =>
      session.execute(cql(metadata))
    )
  }
}
