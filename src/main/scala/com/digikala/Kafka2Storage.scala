package com.digikala

import java.io.File
import java.text.SimpleDateFormat
import java.util
import java.util.Properties
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.EventTransformer
import com.typesafe.config.ConfigFactory
import net.liftweb.json._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.neo4j.driver.v1.Values.parameters
import org.neo4j.driver.v1._
import scala.collection.JavaConverters._
import org.apache.logging.log4j.scala.Logging



class Neo4jLoader {

  val config = ConfigFactory.parseFile(new File("config.conf"))
  val driver = GraphDatabase.driver(config.getString("neo4j.url"), AuthTokens.basic(config.getString("neo4j.username"), config.getString("neo4j.password")))
  val session = driver.session

  def  save(jsonMap : Map[String,Any])={
    val dateFormat : SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
    var parsedDate_dvce_created_tstamp : java.util.Date = dateFormat.parse(jsonMap.getOrElse("dvce_created_tstamp", null).asInstanceOf[String].dropRight(1).replace('T', ' '))
    var timestamp_dvce_created_tstamp :java.sql.Timestamp  = new java.sql.Timestamp(parsedDate_dvce_created_tstamp.getTime())
    session.run("CREATE (a:Page {location: {location}, sessionId: {sessionId} , referer: {referer} , time: {time}})",
      parameters("location", jsonMap.getOrElse("page_url",null).asInstanceOf[String], "sessionId", jsonMap.getOrElse("domain_userid",null).asInstanceOf[String], "referer", jsonMap.getOrElse("page_referrer",null).asInstanceOf[String], "time", jsonMap.getOrElse("dvce_created_tstamp", null).asInstanceOf[String]))
    session.run("MATCH (a:Page),(b:Page) WHERE a.referer = b.location AND a.sessionId = b.sessionId AND b.time < a.time AND a.location ={page_url} and a.sessionId = {session}" + "MERGE (b)-[r:WENT_TO]->(a)", parameters("page_url",jsonMap.getOrElse("page_url",null).toString,"session",jsonMap.getOrElse("domain_userid",null).toString))
  }
  def terminate(session: Session, driver: Driver){
    session.close()
    driver.close()
  }
}

class Neo4jThread extends Thread{

  val config = ConfigFactory.parseFile(new File("config.conf"))
  var jValue : JValue = null
  var neo4j = new Neo4jLoader

  def jsonParser(input : scala.Predef.String) : Map[String,Any] = {
    val event = EventTransformer.transform(input)
    event.right.foreach(
      pair=>
        jValue = parse(pair)
    )
    val jsonMap = jValue.values.asInstanceOf[Map[String, Any]]
    return jsonMap
  }

  val props = new Properties()
  props.put("bootstrap.servers", config.getString("kafka.brokers") )
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", config.getString("neo4j.groupid") )
  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(util.Collections.singletonList(config.getString("kafka.topic")))

  override def run(){

    while (true) {
      val records = consumer.poll(100)
      for (record <- records.asScala) {
        neo4j.save(jsonParser(record.value()))
      }
    }
  }

}

object Kafka2Storage extends Logging {
  def main(args: Array[String]) {
    logger.info("Starting Applicaiton")
    val config = ConfigFactory.parseFile(new File("config.conf"))
    for (i <- 0 until config.getString("neo4j.numberOfThreads").asInstanceOf[Int]) {
      var neo4j = new Neo4jThread
      neo4j.run()
      logger.info("neo4j thread is running")
    }
  }
}