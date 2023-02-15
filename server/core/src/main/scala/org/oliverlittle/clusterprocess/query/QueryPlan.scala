package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.worker_query
import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.sources._

import scala.collection.mutable.{Queue, ArrayBuffer}

object QueryPlan:
    /**
      * Creates a query plan to construct a given table
      *
      * @param output The table to generate a query plan for
      * @return A pair containing (query items to calculate the table, query items to cleanup after the table has been read)
      */
    def apply(output : Table) : (Seq[QueryPlanItem], Seq[QueryPlanItem]) = {
        // Uses a mutable List and Queue
        val queue : Queue[Seq[QueryableObject]] = Queue(getDependencyTree(output)*)
        val list : ArrayBuffer[QueryPlanItem] = ArrayBuffer()

        var nextPostItems : Seq[QueryPlanItem] = Seq()
        while (!queue.isEmpty) {
            // The current set of dependencies to calculate
            val queryObjects : Seq[QueryableObject] = queue.dequeue
            // The post clean-up items for this set of dependencies
            var postItems : Seq[QueryPlanItem] = Seq()
            
            queryObjects.foreach(qo =>
                // Add on the items for the query plan
                list ++= qo.queryItems
                // Add to the set of clean-up items
                postItems ++= qo.cleanupItems
            )

            // Add the clean-up items to the query plan
            list ++= nextPostItems
            // Store the current clean-up items as the next ones
            nextPostItems = postItems
        }

        // Return as immutable List
        return (list.toList, nextPostItems)
    }

    /**
      * Generates the dependency tree for a given table
      * This uses a breadth-first approach, where all tables at depth n - 1 are calculated and held in memory, then depth n is calculated, then depth n - 1 is deleted from memory.
      * A depth-first approach would likely be more efficient, but at the moment it is not possible for any data source to have more than one dependency, so there is no impact right now.
      *
      * @param table
      * @return A list of list of QueryableObjects, which should be evaluated in order - items in the same sub-Seq are dependent at the same time, and cleanup should be run at the end of the next Seq
      */
    def getDependencyTree(table : Table) : Seq[Seq[QueryableObject]] = {
        val executionOrder : ArrayBuffer[Seq[QueryableObject]] = ArrayBuffer()
        var next : Option[Seq[Table]] = Some(Seq(table))
        while (next.isDefined) {
            val tables = next.get
            // For each table in this set of dependencies, add the table, and its data source as dependencies
            executionOrder += tables.flatMap(t => Seq(QueryableTable(t), QueryableDataSource(t.dataSource)))
            
            // Get all dependencies from all tables at this 
            val deps = tables.flatMap(t => t.dataSource.getDependencies)
            next = if deps.size > 0 then Some(deps) else None
        }

        // Reverse the list before returning, because we've actually found the tree from the goal back to the roots
        return executionOrder.reverse.toList
    }

// Helper classes for generating query plans
trait QueryableObject:
    def queryItems : Seq[QueryPlanItem]
    def cleanupItems : Seq[QueryPlanItem]

    given Conversion[Table, QueryableTable] with
        def apply(t : Table) : QueryableTable = QueryableTable(t)

    given Conversion[DataSource, QueryableDataSource] with
        def apply(d : DataSource) : QueryableDataSource = QueryableDataSource(d)
        
case class QueryableTable(t : Table) extends QueryableObject:
    val queryItems = Seq(PrepareResult(t))
    val cleanupItems = Seq(DeleteResult(t))

case class QueryableDataSource(d : DataSource) extends QueryableObject:
    val queryItems = Seq(PreparePartition(d), GetPartition(d))
    val cleanupItems = Seq(DeletePartition(d))


sealed trait QueryPlanItem

// Execute will be calculating the table from data source and storing it back in the tablestore
case class PrepareResult(table : Table) extends QueryPlanItem

// Execute will be removing the item from the tablestore
case class DeleteResult(table : Table) extends QueryPlanItem

// Execute will be negotiating with all other workers to get their partition data (for this partition), then combining them and storing in the tablestore
case class PreparePartition(dataSource : DataSource) extends QueryPlanItem

// Get partition data from other workers
case class GetPartition(dataSource : DataSource) extends QueryPlanItem

// Clear prepared parition data from each worker
case class DeletePartition(dataSource : DataSource) extends QueryPlanItem
