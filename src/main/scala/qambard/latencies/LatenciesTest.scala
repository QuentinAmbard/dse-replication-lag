package qambard.latencies

import java.util
import java.util.UUID
import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import com.datastax.driver.dse.DseCluster
import com.google.common.util.concurrent.{FutureCallback, Futures}
import qambard.latencies.LatenciesTest.rowSelected

import scala.collection.mutable.ArrayBuffer

/**
  * Run test with
  * java -Dlatency.precisionInMs=2 -Dlatency.password=cassandra -Dlatency.user=cassandra -Dlatency.ks=replication_test -Dlatency.dc1.contactPoint=35.238.71.176 -Dlatency.dc2.contactPoint=35.192.97.184 -Dlatency.dc1.name=dc1 -Dlatency.dc2.name=dc2 -Dlatency.resetCount=200 -jar /home/quentin/projects/latencies/target/demo-0.1-SNAPSHOT-jar-with-dependencies.jar
  */


class ResultCallback(startInsertTime: Long, startQueryTime: Long) extends FutureCallback[ResultSet] {
  override def onFailure(t: Throwable): Unit = {
    t.printStackTrace()
  }

  override def onSuccess(result: ResultSet): Unit = {
    if(!rowSelected) {
      LatenciesTest.rowSelected = true
      val time = System.nanoTime()
      val selectTime = time - startQueryTime
      val replicationTime = time - startInsertTime - selectTime / 2
      Recorders.recorderSelect.recordValue(TimeUnit.NANOSECONDS.toMillis(selectTime))
      //println(s"selectTime=$selectTime, replicationTime=$replicationTime")
      Recorders.recorderReplication.recordValue(TimeUnit.NANOSECONDS.toMillis(replicationTime))
    }
  }
}

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


  private val rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy
  private val executorWaiting = new ThreadPoolExecutor(2000, 2001, 1, TimeUnit.MINUTES, new ArrayBlockingQueue[Runnable](1000 * 2), rejectedExecutionHandler)


  val ks = getProperty("latency.ks", "replication_test")
  val dc1_contactPoint = getProperty("latency.dc1.contactPoint", "localhost").split(",")
  val dc1_name = getProperty("latency.dc1.name", "datacenter1")
  val dc2_contactPoint = getProperty("latency.dc2.contactPoint", "localhost").split(",")
  val dc2_name = getProperty("latency.dc2.name", "datacenter1")
  val resetCount = getProperty("latency.resetCount", "200").toInt
  val user = getProperty("latency.user", "cassandra")
  val password = getProperty("latency.password", "cassandra")
  val precision = getProperty("latency.precisionInMs", "2").toInt

  val sessionDC1 = openConnection(dc1_contactPoint, dc1_name)
  //sessionDC1.execute(s"CREATE KEYSPACE if NOT EXISTS $ks WITH replication = {'class': 'NetworkTopologyStrategy', '$dc1_name': 1, '$dc2_name': 1}")
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
  @volatile
  var rowSelected = false


  def runLatencyTest() = {


    val insertSt = sessionDC1.prepare(s"INSERT INTO $ks.latency (id, value) values (?, ?)").setConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
    val selectSt = sessionDC2.prepare(s"SELECT id from $ks.latency where id=?").setConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
    var i = 0
    while (true) {
      rowSelected = false
      val uuid = UUID.randomUUID()
      val rdText = randomStr(4000)
      val startTime = System.nanoTime()
      sessionDC1.executeAsync(insertSt.bind(uuid, rdText))

      val futures = ArrayBuffer[ResultSetFuture]()
      while (!rowSelected) {
        Thread.sleep(precision)
        val future = sessionDC2.executeAsync(selectSt.bind(uuid))
        Futures.addCallback(future, new ResultCallback(startTime, System.nanoTime()), executorWaiting)
        futures.append(future)
      }
      futures.foreach(_.getUninterruptibly)
      Thread.sleep(precision)
      i += 1
      if (i % resetCount == 0) {
        val p50select = Recorders.recorderSelect.getValueAtPercentile(50.0)
        val p99select = Recorders.recorderSelect.getValueAtPercentile(99.0)
        val p999select = Recorders.recorderSelect.getValueAtPercentile(999)


        val p50replication = Recorders.recorderReplication.getValueAtPercentile(50.0)
        val p99replication = Recorders.recorderReplication.getValueAtPercentile(99.0)
        val p999replication = Recorders.recorderReplication.getValueAtPercentile(999)
        println(s"Replication time estimation: p50=$p50replication, p99=$p99replication, p99.9=$p999replication")
        println(s"select time: p50=$p50select, p99=$p99select, p99.9=$p999select")
        Recorders.recorderSelect.reset()
        Recorders.recorderReplication.reset()
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


