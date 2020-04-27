package com.knoldus.akka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.knoldus.akka.AkkaKafkaConsumer.DeserializationFlowFactory
import com.knoldus.models.Employee

class Backend (esIndex: String,
               esHosts: String, esRestPort: Int)(
                override implicit val actorSystem: ActorSystem,
                implicit val actorMaterializer: ActorMaterializer,
                // Using a synchronous deserialization flow because the sheer multiplicity of streams
                // gives us more than enough parallelism for good throughput
                implicit val dff: DeserializationFlowFactory = DeserializationFlowFactory.Synchronous)
  extends DataFlowToES(esIndex, esHosts, esRestPort) {

  private val writeEmployeeToEs = writeDataFlowToES("employee", Employee)
  def run(): Unit = {
    j(writeEmployeeToEs)
  }
}
