package com.aktripathi.all

import java.net.InetAddress

import org.elasticsearch.action.bulk.{BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.transport.client.PreBuiltTransportClient


object ESConnection {

  var dyn_transport_settings  = Settings.builder()
  dyn_transport_settings.put("cluster.name","cluster")

  lazy val settings: Settings = {
    Settings.builder().put(dyn_transport_settings.build()).build()
  }

  lazy val client = {
    new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("elasticsearch"),
      9300))
  }

  val bl = new BulkProcessor.Listener {

    override def beforeBulk(executionId: Long, request: BulkRequest): Unit = {}

    override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit = {}

    override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit = {}

  }
  val  bp : BulkProcessor = BulkProcessor.builder(client, bl).setFlushInterval(TimeValue.timeValueSeconds(1)).build()

  def write(idx: String,
            typ: String,
            str: String): Unit = {

    if (!Option(client).isEmpty) {
      bp.add(new IndexRequest(idx, typ)
        .source(str))
    }
  }
}
