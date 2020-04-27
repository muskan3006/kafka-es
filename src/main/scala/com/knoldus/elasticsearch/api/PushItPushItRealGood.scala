package com.knoldus.elasticsearch.api

import akka.actor.ActorSystem
import com.knoldus.akka.{Backend, CreateIndex}
import com.knoldus.common.AkkaStreamJob
object PushItPushItRealGood extends AkkaStreamJob(ActorSystem("Salt-N-Pepa")) {

  private val esHosts = "localhost"
    //config.getString("elasticsearch.cluster.hosts.storeserv")
  private val esRestPort = 9200
  //config.getString("elasticsearch.cluster.port.storeserv").toInt
  private val esIndex: String = "employee-index"

  def main(args: Array[String]): Unit = {
    //val starvationDetectorSettings = StarvationDetectorSettings.fromConfig(actorSystem.settings.config.getConfig("akka.diagnostics.starvation-detector"))
//    val kafkaDispatcher = actorSystem.dispatchers.lookup("kafka-dispatcher")
   // StarvationDetector.checkExecutionContext(kafkaDispatcher, actorSystem.log, starvationDetectorSettings, () => false)
    CreateIndex.run()
     val b = new Backend(esIndex,esHosts,esRestPort)
      b.run()
  }
}
//
//object Driver extends App with AkkaStreamJob(ActorSystem("akka-stream")){
//  implicit val ec:ExecutionContext=ExecutionContext.global
//  implicit val system: ActorSystem = ActorSystem.create()
//
//  implicit val mat: ActorMaterializer = ActorMaterializer()
//
// // private val elasticsearchCreateIndex = config.getBoolean("elasticsearch.create-index")
//  private val esHosts = "localhost"
//    //config.getString("elasticsearch.cluster.hosts.storeserv")
//  private val esRestPort = 9200
//    //config.getString("elasticsearch.cluster.port.storeserv").toInt
//  private val esIndex: String = "employee-index"
//    //config.getString("elasticsearch.index.storeserv")
//
//  CreateIndex.run()
//  val b = new Backend(esIndex,esHosts,esRestPort)
//  b.run()
//}
