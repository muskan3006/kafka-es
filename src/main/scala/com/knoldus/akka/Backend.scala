package com.knoldus.akka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.knoldus.akka.AkkaKafkaConsumer.DeserializationFlowFactory
import com.knoldus.models.{Company, Employee}

class Backend(esIndex: String,
              esHosts: String, esRestPort: Int)(
               override implicit val actorSystem: ActorSystem,
               implicit val actorMaterializer: ActorMaterializer,
               implicit val dff: DeserializationFlowFactory = DeserializationFlowFactory.Synchronous)
  extends DataFlowToES(esIndex, esHosts, esRestPort) {

  private val writeEmployeeToEs = writeDataFlowToES("emp", Employee)
private val writeCompanyToEs = writeDataFlowToES("comp",Company)
  def run(): Unit = {
    topicAndFlow(writeEmployeeToEs)
    topicAndFlow(writeCompanyToEs)
  }
}
