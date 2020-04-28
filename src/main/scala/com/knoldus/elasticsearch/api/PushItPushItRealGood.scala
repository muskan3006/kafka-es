package com.knoldus.elasticsearch.api

import akka.actor.ActorSystem
import com.knoldus.akka.{Backend, CreateIndex}
import com.knoldus.common.AkkaStreamJob
object PushItPushItRealGood extends AkkaStreamJob(ActorSystem("Salt-N-Pepa")) {

  private val esHosts = config.getString("elasticsearch.host")
  private val esRestPort = config.getInt("elasticsearch.restport")
  private val esIndex: String = config.getString("elasticsearch.index.default")

  def main(args: Array[String]): Unit = {
    CreateIndex.run()
     val b = new Backend(esIndex,esHosts,esRestPort)
      b.run()
  }
}
