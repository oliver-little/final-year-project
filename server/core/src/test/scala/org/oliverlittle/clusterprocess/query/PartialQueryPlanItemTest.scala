package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.AsyncUnitSpec
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.sources._
import org.oliverlittle.clusterprocess.model.field.expressions._

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.RecoverMethods._

import akka.actor.testkit.typed.scaladsl.BehaviorTestKit
import akka.actor.testkit.typed.scaladsl.TestInbox
import akka.actor.typed._
import akka.actor.typed.scaladsl._
import akka.util.Timeout
import akka.actor.typed.scaladsl.AskPattern._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, ExecutionContext}

given Timeout = 10.seconds

abstract class StoreSpec extends AsyncUnitSpec with BeforeAndAfterAll with BeforeAndAfterEach {
    var system : ActorSystem[TableStoreSystem.TableStoreSystemEvent] = null
    var store : ActorRef[TableStore.TableStoreEvent] = null

    given ActorSystem[TableStoreSystem.TableStoreSystemEvent] = system
    given ExecutionContext = system.executionContext

    override protected def beforeAll() : Unit = {
        system = TableStoreSystem.create()
        store = Await.result(system.ask(ref => TableStoreSystem.GetStore(ref)), 10.seconds)
    }

    override protected def afterEach() : Unit = {
        store ! TableStore.Reset()
    }

    override protected def afterAll() : Unit = {
        system.terminate()
    }
}

class PartialPrepareResultTest extends StoreSpec {
    "A PartialPrepareResult" should "add a PartialTable to the TableStore" in {
        val ds = MockPartialDataSource()
        val partialTable = PartialTable(ds, Seq(SelectTransformation(F("a") as "b")))
        // Add dependent partition
        store.ask(ref => TableStore.AddPartition(ds, ds.sampleResult, ref)) flatMap { res =>
            // Then execute result
            val item = PartialPrepareResult(partialTable)
            item.execute(store)
        } flatMap {res =>
            // Check results
            res should be (worker_query.ProcessQueryPlanItemResult(true))
            store.ask[TableStoreData](ref => TableStore.GetData(ref)) map { tableStoreData =>
                tableStoreData.tables(partialTable.parent)(partialTable) should be (LazyTableResult(partialTable.parent.outputHeaders, ds.sampleResult.rows))
            }
        }
    }

    it should "throw an error when the dependent PartialDataSource is missing" in {
        val ds = MockPartialDataSource()
        val partialTable = PartialTable(ds, Seq(SelectTransformation(F("a") as "b")))

        recoverToSucceededIf[IllegalArgumentException] {
            val item = PartialPrepareResult(partialTable)
            item.execute(store)
        }
    }
}

class PartialDeleteResultTest extends StoreSpec {
    "A PartialDeleteResult" should "remove all instances of a table from the TableStore" in {
        val ds = MockPartialDataSource()
        val partialTable = PartialTable(ds, Seq(SelectTransformation(F("a") as "b")))
        val tableResult = LazyTableResult(partialTable.parent.outputHeaders, ds.sampleResult.rows)
        store.ask(ref => TableStore.AddResult(partialTable, tableResult, ref)) flatMap { res =>
            val item = PartialDeleteResult(partialTable.parent)
            item.execute(store)
        } flatMap {res =>
            res should be (worker_query.ProcessQueryPlanItemResult(true))
            store.ask[TableStoreData](ref => TableStore.GetData(ref)) map { tableStoreData =>
                tableStoreData.tables.contains(partialTable.parent) should be (false)
            }
        }
    }
}

class PartialPrepareHashesTest extends StoreSpec {
    "A PartialPrepareHashes" should "calculate the hashes of the dependencies of a data source" in {
        val ds = MockPartialDataSource()
        val partialTable = PartialTable(ds, Seq(SelectTransformation(F("a") as "b")))
        val tableResult = LazyTableResult(partialTable.parent.outputHeaders, ds.sampleResult.rows)

        // Get dependency that will be hashed
        val dependency = ds.parent.getDependencies(0)
        val dependencyPartialTable = PartialTable(dependency.dataSource.asInstanceOf[MockDataSource].partial, Seq())

        // Add a fake result for the dependency
        store.ask(ref => TableStore.AddResult(dependencyPartialTable, ds.sampleResult, ref)) flatMap { res =>
            val item = PartialPrepareHashes(ds.parent, 2)
            item.execute(store)
        } flatMap {res =>
            res should be (worker_query.ProcessQueryPlanItemResult(true))
            // Check that the dependency hash function was called
            store.ask[TableStoreData](ref => TableStore.GetData(ref)) map { tableStoreData =>
                tableStoreData.hashes((dependency, 2)) should be (ds.parent.partitionHash)
            }
        }
    }

    it should "continue even if dependencies are missing" in {
        // In this case, we are checking that the TableStore ignores any missing dependencies
        val ds = MockPartialDataSource()
        val partialTable = PartialTable(ds, Seq(SelectTransformation(F("a") as "b")))
        val item = PartialPrepareHashes(ds.parent, 2)
        item.execute(store)

        // Get dependency that will be hashed
        val dependency = ds.parent.getDependencies(0)

        store.ask[TableStoreData](ref => TableStore.GetData(ref)) map { tableStoreData =>
            tableStoreData.hashes.contains((dependency, 2)) should be (false)
        }
    }
}

class PartialDeletePreparedHashesTest extends StoreSpec {
    "A PartialDeletePreparedHashes" should "remove all calculated hashes for a DependentDataSource" in {
        val ds = MockPartialDataSource()
        val partialTable = PartialTable(ds, Seq(SelectTransformation(F("a") as "b")))
        val tableResult = LazyTableResult(partialTable.parent.outputHeaders, ds.sampleResult.rows)

        // Get dependency that will be hashed
        val dependency = ds.parent.getDependencies(0)
        val dependencyPartialTable = PartialTable(dependency.dataSource.asInstanceOf[MockDataSource].partial, Seq())

        // Add a fake result for the dependency
        store.ask(ref => TableStore.AddResult(dependencyPartialTable, ds.sampleResult, ref)) flatMap {res =>
            PartialPrepareHashes(ds.parent, 2).execute(store)
        } flatMap {res =>
            res should be (worker_query.ProcessQueryPlanItemResult(true))
            // Check that the dependency hash function was called
            store.ask[TableStoreData](ref => TableStore.GetData(ref)) map { tableStoreData =>
                tableStoreData.hashes((dependency, 2)) should be (ds.parent.partitionHash)
            }
        } flatMap {res =>
            PartialDeletePreparedHashes(ds.parent, 2).execute(store)
        } flatMap {res =>
            res should be (worker_query.ProcessQueryPlanItemResult(true))
            // Check that the deletion occurred
            store.ask[TableStoreData](ref => TableStore.GetData(ref)) map { tableStoreData =>
                tableStoreData.hashes.contains((dependency, 2)) should be (false)
            }
        }
    }
}

class PartialGetPartitionTest extends StoreSpec {
    "A PartialGetPartition" should "add a PartialDataSource to the TableStore" in {
        val ds = MockPartialDataSource()

        PartialGetPartition(ds, Seq()).execute(store) flatMap {res =>
            res should be (worker_query.ProcessQueryPlanItemResult(true))
            // Check that the data was stored
            store.ask[TableStoreData](ref => TableStore.GetData(ref)) map { tableStoreData =>
                tableStoreData.partitions(ds.parent)(ds) should be (ds.partialData)
            }
        }
    }
}

class PartialDeletePartitionTest extends StoreSpec {
    "A PartialDeletePartition" should "delete all instances of a DataSource from the TableStore" in {
        val ds = MockPartialDataSource()

        PartialGetPartition(ds, Seq()).execute(store) flatMap {res =>
            res should be (worker_query.ProcessQueryPlanItemResult(true))
            // Check that the data was stored
            store.ask[TableStoreData](ref => TableStore.GetData(ref)) map { tableStoreData =>
                tableStoreData.partitions(ds.parent)(ds) should be (ds.partialData)
            }
        } flatMap {res =>
            PartialDeletePartition(ds.parent).execute(store)    
        } flatMap {res =>
            res should be (worker_query.ProcessQueryPlanItemResult(true))
            // Check that the data was stored
            store.ask[TableStoreData](ref => TableStore.GetData(ref)) map { tableStoreData =>
                tableStoreData.partitions.contains(ds.parent) should be (false)
            }
        }
    }
}