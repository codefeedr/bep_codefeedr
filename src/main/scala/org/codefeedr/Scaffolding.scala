package org.codefeedr

import org.apache.flink.api.java.operators.{DataSink, DataSource}
import org.apache.flink.streaming.api.scala.DataStream


class None {
}

////////// CONTEXT/ENV

class Context {

  // get Streaming Environment

  // register storages
  // register pipelines
  // register buffers

  // pipeline builder

  // buffer configuration
  // storage configuraion

}

////////// PIPELINE

abstract class PipelineObject[In, Out] {

//  def getStorageSource[T](typ: String, collection: String): DataStream[T] = {
//
//  }
//
//  def getStorageSink[T](typ: String, collection: String): DataSink[T] = {
//
//  }
//
//  def getSource(): DataStream[In] = {
//
//  }
//
//  def getSink(): DataSink[Out] = {
//
//  }

  def setup(): Unit
  def main(): Unit
}

abstract class Job[In] extends PipelineObject[In, None] {

  def setup(): Unit = {
  }

  def start(): Unit = {
    println("Start the CF Job")
  }

}

trait Buffer[T] {
  def getSource(): DataStream[T]

  def getSink(): DataSink[T]
}

//class KafkaBuffer[T] extends Buffer[T] {
//  def getSource(): DataStream[T] = {
//
//  }
//
//  def getSink(): DataSink[T] = {
//
//  }
//}

////////// STORAGE

trait Storage[T] {
  def getSource(collection: String): DataStream[T]

  def getSink(collection: String): DataSink[T]
}

////////// KV STORE

trait KeyValueStore {
  def get(key: String): String

  def set(key: String, value: String): Unit

  def has(key: String): Boolean

  // def watch
}

trait KeyManager extends KeyValueStore {

}

class StaticKeyManager extends KeyManager {
  def get(key: String): String = {
    "none"
  }

  def set(key: String, value: String) {
  }

  def has(key: String): Boolean = {
    false
  }
}

class ZookeeperStore {

}

class ZookeeperKeyManager extends ZookeeperStore with KeyManager {
  def get(key: String): String = {
    "none"
  }

  def set(key: String, value: String) {
  }

  def has(key: String): Boolean = {
    false
  }
}


////////// EXAMPLE

object Main2 {
  def main(args: Array[String]): Unit = {

    val job = new MyJob()
    job.start()
  }
}

case class Person(name: String, age: Int)

class MyJob extends Job[Person] {

  def main(): Unit = {
    println("Job Main")
  }

}