/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.codefeedr.pipeline

import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.stages.utilities.StringType
import org.apache.flink.streaming.api.scala._
import org.codefeedr.stages.{InputStage, OutputStage, OutputStage2}
import org.scalatest.FunSuite

class DirectedAcyclicGraphTest extends FunSuite {

  val nodeA = "a"
  val nodeB = "b"
  val nodeC = "c"
  val nodeD = "d"

  class NothingStringType extends InputStage[StringType] {
    override def main(context: Context): DataStream[StringType] = {
      context.env.fromCollection(Seq(StringType("a")))
    }
  }

  class StringTypeStringTypeNothing
      extends OutputStage2[StringType, StringType] {
    override def main(source: DataStream[StringType],
                      secondSource: DataStream[StringType]): Unit = Unit
  }

  class StringTypeNothing extends OutputStage[StringType] {
    override def main(source: DataStream[StringType]) = Unit
  }

  class IntTypeNothing extends OutputStage[IntType] {
    override def main(source: DataStream[IntType]) = Unit
  }

  class NothingIntType extends InputStage[IntType] {
    override def main(context: Context): DataStream[IntType] = null
  }

  test("Added nodes are testable") {
    val dag = new DirectedAcyclicGraph()
      .addNode(nodeA)

    assert(dag.hasNode(nodeA))
    assert(!dag.hasNode(nodeB))
  }

  test("Added edges are added") {
    val dag = new DirectedAcyclicGraph()
      .addNode(nodeA)
      .addNode(nodeB)
      .addEdge(nodeA, nodeB)

    assert(dag.hasEdge(nodeA, nodeB))
  }

  test("Adding edges for nodes that do not exist throws") {
    val dag = new DirectedAcyclicGraph()
      .addNode(nodeA)

    assertThrows[IllegalArgumentException] {
      dag.addEdge(nodeA, nodeB)
    }

    assertThrows[IllegalArgumentException] {
      dag.addEdge(nodeC, nodeA)
    }
  }

  test("Adding an edge that causes a cycle throws") {
    val dag = new DirectedAcyclicGraph()
      .addNode(nodeA)
      .addNode(nodeB)
      .addNode(nodeC)
      .addEdge(nodeA, nodeB)
      .addEdge(nodeB, nodeC)

    assertThrows[IllegalArgumentException] {
      dag.addEdge(nodeC, nodeA)
    }
  }

  test("Parents are retrievable") {
    val dag = new DirectedAcyclicGraph()
      .addNode(nodeA)
      .addNode(nodeB)
      .addNode(nodeC)
      .addEdge(nodeA, nodeC)
      .addEdge(nodeB, nodeC)

    assert(dag.getParents(nodeC).toSet == Set(nodeA, nodeB))
  }

  test("Children are retrievable") {
    val dag = new DirectedAcyclicGraph()
      .addNode(nodeA)
      .addNode(nodeB)
      .addNode(nodeC)
      .addEdge(nodeA, nodeC)
      .addEdge(nodeB, nodeC)

    assert(dag.getChildren(nodeA).toSet == Set(nodeC))
    assert(dag.getChildren(nodeB).toSet == Set(nodeC))
  }

  test("Adding an existing edge does not affect the graph") {
    val dag = new DirectedAcyclicGraph()
      .addNode(nodeA)
      .addNode(nodeB)
      .addEdge(nodeA, nodeB)

    val dag2 = dag.addEdge(nodeA, nodeB)

    assert(dag == dag2)
  }

  test("Sequential DAG should be detected") {
    val dag = new DirectedAcyclicGraph()
      .addNode(nodeA)
      .addNode(nodeB)
      .addNode(nodeC)
      .addEdge(nodeA, nodeB)
      .addEdge(nodeB, nodeC)

    assert(dag.isSequential)

    assert(dag.lastInSequence.get == nodeC)
  }

  test("Non-sequential DAG should be detected (1)") {
    val dag = new DirectedAcyclicGraph()
      .addNode(nodeA)
      .addNode(nodeB)
      .addNode(nodeC)
      .addNode(nodeD)
      .addEdge(nodeA, nodeB)
      .addEdge(nodeA, nodeC)
      .addEdge(nodeC, nodeD)
      .addEdge(nodeB, nodeD)

    assert(!dag.isSequential)
    assert(dag.lastInSequence.isEmpty)
  }

  test("Non-sequential DAG should be detected (2)") {
    val dag = new DirectedAcyclicGraph()
      .addNode(nodeA)
      .addNode(nodeB)
      .addNode(nodeC)
      .addNode(nodeD)
      .addEdge(nodeA, nodeB)
      .addEdge(nodeA, nodeC)
      .addEdge(nodeC, nodeD)

    assert(!dag.isSequential)
  }

  test("Non-sequential DAG should be detected (3)") {
    val dag = new DirectedAcyclicGraph()
      .addNode(nodeA)
      .addNode(nodeB)
      .addNode(nodeC)
      .addNode(nodeD)
      .addEdge(nodeA, nodeB)
      .addEdge(nodeB, nodeC)
      .addEdge(nodeD, nodeC)

    assert(!dag.isSequential)
  }

  test("A DAG is non-sequential when the graph is split") {
    val dag = new DirectedAcyclicGraph()
      .addNode(nodeA)
      .addNode(nodeB)
      .addNode(nodeC)
      .addNode(nodeD)
      .addEdge(nodeA, nodeB)
      .addEdge(nodeC, nodeD)

    assert(!dag.isSequential)
  }

  test("Any orphans will be removed") {
    val dag = new DirectedAcyclicGraph()
      .addNode(nodeA)
      .addNode(nodeB)
      .addNode(nodeC)
      .addNode(nodeD)
      .addEdge(nodeA, nodeB)
      .addEdge(nodeA, nodeC)

    assert(dag.withoutOrphans.nodes.toSet == Set(nodeA, nodeB, nodeC))
  }

  test("Equality operator") {
    val dag = new DirectedAcyclicGraph()
    val dag2 = new DirectedAcyclicGraph()
    val dag3 = new DirectedAcyclicGraph().addNode(nodeA)

    assert(dag == dag2)
    assert(dag2 != dag3)
    assert(dag != "hello")
  }

  test("Empty DAG is sequential") {
    val dag = new DirectedAcyclicGraph()

    assert(dag.isSequential)
  }

  test("Last in sequence of empty dag") {
    val dag = new DirectedAcyclicGraph()

    assert(dag.lastInSequence.isEmpty)
  }

  test("Edges are ordered") {
    val dag = new DirectedAcyclicGraph()
      .addNode(nodeA)
      .addNode(nodeB)
      .addNode(nodeC)
      .addEdge(nodeA, nodeB)
      .addEdge(nodeC, nodeB)

    val parents = dag.getParents(nodeB)
    val firstParent = dag.getFirstParent(nodeB)

    assert(firstParent.isDefined)
    assert(firstParent.get == nodeA)
    assert(parents.size == 2)
    assert(parents(0) == nodeA)
    assert(parents(1) == nodeC)
  }

  test("Getting first parent of empty dag gives None") {
    val dag = new DirectedAcyclicGraph()

    assert(dag.getFirstParent(null).isEmpty)
  }

  test("Verify if a graph is empty") {
    val dag = new DirectedAcyclicGraph()

    assert(dag.isEmpty)
    assert(!dag.addNode(nodeA).isEmpty)
  }

  test("Verify that graph is type in-compatible") {
    val stageOne = new NothingStringType
    val stageTwo = new IntTypeNothing

    val dag = new DirectedAcyclicGraph()
      .addNode(stageOne)
      .addNode(stageTwo)
      .addEdge(stageOne, stageTwo)

    assertThrows[StageTypesIncompatibleException] {
      dag.verify()
    }
  }

  test("Verify that graph is type compatible") {
    val stageOne = new NothingStringType
    val stageTwo = new StringTypeNothing

    val dag = new DirectedAcyclicGraph()
      .addNode(stageOne)
      .addNode(stageTwo)
      .addEdge(stageOne, stageTwo)

    dag.verify()
  }

  test("Verify that graph is invalid; inputstage has incoming edge.") {
    val stageOne = new NothingStringType
    val stageTwo = new IntTypeNothing

    val dag = new DirectedAcyclicGraph()
      .addNode(stageOne)
      .addEdge(stageOne, stageOne)

    assertThrows[InvalidPipelineException] {
      dag.verify()
    }
  }

  test("Verify that graph is invalid: outputstage has outgoing edges.") {
    val stageTwo = new IntTypeNothing

    val dag = new DirectedAcyclicGraph()
      .addNode(stageTwo)
      .addEdge(stageTwo, stageTwo)

    assertThrows[InvalidPipelineException] {
      dag.verify()
    }
  }

  test("Verify that stages are incompatible; 2 outputs, 1 input.") {
    val stageOne = new NothingStringType
    val stageTwo = new NothingStringType
    val stageThree = new StringTypeStringTypeNothing

    val dag = new DirectedAcyclicGraph()
      .addNode(stageOne)
      .addNode(stageThree)
      .addEdge(stageOne, stageThree) // we only add 1 edge

    assertThrows[StageTypesIncompatibleException] {
      dag.verify()
    }
  }

  test("Verify that stages are compatible; 2 outputs, 2 input.") {
    val stageOne = new NothingStringType
    val stageTwo = new NothingStringType
    val stageThree = new StringTypeStringTypeNothing

    val dag = new DirectedAcyclicGraph()
      .addNode(stageOne)
      .addNode(stageTwo)
      .addNode(stageThree)
      .addEdge(stageTwo, stageThree)
      .addEdge(stageOne, stageThree)

    dag.verify()
  }

  test(
    "Verify that stages are in-compatible; 2 outputs, 2 input but at least 1 wrong type.") {
    val stageOne = new NothingStringType
    val stageTwo = new NothingIntType
    val stageThree = new StringTypeStringTypeNothing

    val dag = new DirectedAcyclicGraph()
      .addNode(stageOne)
      .addNode(stageTwo)
      .addNode(stageThree)
      .addEdge(stageTwo, stageThree)
      .addEdge(stageOne, stageThree)

    assertThrows[StageTypesIncompatibleException] {
      dag.verify()
    }
  }

}
