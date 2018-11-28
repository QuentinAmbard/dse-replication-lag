package qambard.latencies

import java.nio.ByteBuffer
import java.util
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.datastax.driver.core.{ConsistencyLevel, PlainTextAuthProvider, Row}
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import com.datastax.driver.dse.DseCluster
import org.HdrHistogram.Histogram

/**
  * Run test with
  * java -Dlatency.password=cassandra -Dlatency.user=cassandra -Dlatency.ks=replication_test -Dlatency.dc1.contactPoint=35.238.71.176 -Dlatency.dc2.contactPoint=35.192.97.184 -Dlatency.dc1.name=dc1 -Dlatency.dc2.name=dc2 -Dlatency.resetCount=200 -jar /home/quentin/projects/latencies/target/demo-0.1-SNAPSHOT-jar-with-dependencies.jar
  */
object LatenciesTest extends App {

  def getProperty(value: String, default: String = null): String = System.getProperty(value) match {
    case null => default
    case c => c
  }

  val random = new util.Random

  val subset: Array[Char] = "0123456789abcdefghijklmnopqrstuvwxyzAZERTYUIOPMLKJHGFDSQWXCVBN".toCharArray
  def randomStr(length: Int): String = {
    val buf = new Array[Char](length)
    for (i <- 0 to buf.length - 1) {
      val index = random.nextInt(subset.length)
      buf(i) = subset(index)
    }
    new String(buf)
  }


  val ks = getProperty("latency.ks", "replication_test")
  val dc1_contactPoint = getProperty("latency.dc1.contactPoint", "localhost").split(",")
  val dc1_name = getProperty("latency.dc1.name", "datacenter1")
  val dc2_contactPoint = getProperty("latency.dc2.contactPoint", "localhost").split(",")
  val dc2_name = getProperty("latency.dc2.name", "datacenter1")
  val resetCount = getProperty("latency.resetCount", "200").toInt
  val user = getProperty("latency.user", "cassandra")
  val password = getProperty("latency.password", "cassandra")

  val sessionDC1 = openConnection(dc1_contactPoint, dc1_name)
  sessionDC1.execute(s"CREATE KEYSPACE if NOT EXISTS $ks WITH replication = {'class': 'NetworkTopologyStrategy', '$dc1_name': 1, '$dc2_name': 1}")
  sessionDC1.execute(s"CREATE TABLE if NOT EXISTS $ks.latency (id uuid PRIMARY KEY , value text)")
  val sessionDC2 = openConnection(dc2_contactPoint, dc2_name)
  try {
    runLatencyTest()
  } catch {
    case e: Throwable => {
      e.printStackTrace()
      sessionDC1.getCluster.close()
      sessionDC2.getCluster.close()
    }
  }
  def runLatencyTest() = {
    val numberOfSignificantValueDigits = 3
    val highestTrackableValue = 60000
    val recorderReplication = new Histogram(highestTrackableValue, numberOfSignificantValueDigits)
    val recorderInsert = new Histogram(highestTrackableValue, numberOfSignificantValueDigits)
    val recorderSelect = new Histogram(highestTrackableValue, numberOfSignificantValueDigits)


    val insertSt = sessionDC1.prepare(s"INSERT INTO $ks.latency (id, value) values (?, ?)").setConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
    val selectSt = sessionDC2.prepare(s"SELECT id from $ks.latency where id=?").setConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
    var i = 0
    while (true) {
      val uuid = UUID.randomUUID()
      val rdText = randomStr(4000)
      val startTime = System.nanoTime()
      sessionDC1.execute(insertSt.bind(uuid, rdText))
      val insertTime = System.nanoTime() - startTime
      var selectTime = 0L
      var result: Row = null
      while (result == null) {
        val startQueryTime = System.nanoTime()
        result = sessionDC2.execute(selectSt.bind(uuid)).one()
        selectTime = System.nanoTime() - startQueryTime
        recorderSelect.recordValue(TimeUnit.NANOSECONDS.toMillis(selectTime))
      }
      val retrieveTime = System.nanoTime()
      //Total time to do the insert + the retrieving time
      val cycleTime = retrieveTime - startTime
      //Replication time estimation would be this time minus (insert-time/2 + select-time/2) to remove query network latencies
      val replicationTime = cycleTime - selectTime / 2 - insertTime / 2
      recorderReplication.recordValue(TimeUnit.NANOSECONDS.toMillis(replicationTime))
      recorderInsert.recordValue(TimeUnit.NANOSECONDS.toMillis(insertTime))
      i += 1
      if (i % resetCount == 0) {
        val p50insert = recorderInsert.getValueAtPercentile(50.0)
        val p99insert = recorderInsert.getValueAtPercentile(99.0)
        val p999insert = recorderInsert.getValueAtPercentile(999)

        val p50select = recorderInsert.getValueAtPercentile(50.0)
        val p99select = recorderInsert.getValueAtPercentile(99.0)
        val p999select = recorderInsert.getValueAtPercentile(999)

        val p50replication = recorderInsert.getValueAtPercentile(50.0)
        val p99replication = recorderInsert.getValueAtPercentile(99.0)
        val p999replication = recorderInsert.getValueAtPercentile(999)
        println(s"Replication time estimation: p50=$p50replication, p99=$p99replication, p99.9=$p999replication")
        println(s"insert time: p50=$p50insert, p99=$p99insert, p99.9=$p999insert")
        println(s"select time: p50=$p50select, p99=$p99select, p99.9=$p999select")
        recorderInsert.reset()
        recorderSelect.reset()
        recorderReplication.reset()
      }
    }

  }

  private def openConnection(contactPoints: Array[String], dcName: String) = {
    println(s"OPENING CONNECTION TO $dcName ($contactPoints)")
    DseCluster.builder().addContactPoints(contactPoints: _*)
      .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().withLocalDc(dcName).build()))
        .withAuthProvider(new PlainTextAuthProvider(user, password))
      .build().connect()
  }
}
