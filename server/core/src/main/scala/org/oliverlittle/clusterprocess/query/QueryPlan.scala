package org.oliverlittle.clusterprocess.query

import org.oliverlittle.clusterprocess.model.table._
import org.oliverlittle.clusterprocess.model.table.sources._

import scala.collection.mutable.{Queue, ArrayBuffer}

object QueryPlan:
    def apply(output : Table) : Seq[QueryPlanItem] = {
        // Uses a mutable List and Queue
        val queue : Queue[Seq[Table]] = Queue(getDependencyTree(output))
        val list : ArrayBuffer[QueryPlanItem] = ArrayBuffer()

        var nextPostItems : Seq[QueryPlanItem] = Seq()
        while (!queue.isEmpty) {
            // The current set of dependencies to calculate
            val tables : Seq[Table] = queue.dequeue
            // The post clean-up items for this set of dependencies
            var postItems : Seq[QueryPlanItem] = Seq()
            
            tables.foreach(t =>
                
                // Get the query plan for this table
                val (items, cleanup) = getQueryPlanForTable(t)

                // Add on the items for the query plan
                list ++= items
                // Add to the set of clean-up items
                postItems ++= cleanup
            )

            // Add the clean-up items to the query plan
            list ++= nextPostItems
            // Store the current clean-up items as the next ones
            nextPostItems = postItems
        }

        list ++= nextPostItems

        // Return as immutable List
        return list.toList
    }

    def getQueryPlanForTable(table : Table) : (Seq[QueryPlanItem], Seq[QueryPlanItem]) = (Seq(PrepareResult(table)), Seq(DeleteResult(table)))

    def getDependencyTree(table : Table) : Seq[Table] = {
        var executionOrder : ArrayBuffer[Table] = ArrayBuffer()
        var tables : Queue[Table] = Queue(table)

        while (!tables.isEmpty) {
            val table = tables.dequeue
            executionOrder += table
            tables.enqueueAll(table.dataSource.getDependencies)
        }

        // Reverse the list before returning, because we've actually found the tree from the goal back to the roots
        return executionOrder.reverse.toList
    }

trait QueryPlanItem: 
    def execute : Unit = {}


// Execute will be calculating the table from data source and storing it back in the tablestore
case class PrepareResult(table : Table) extends QueryPlanItem

// Execute will be removing the item from the tablestore
case class DeleteResult(table : Table) extends QueryPlanItem

// Execute will be negotiating with all other workers to get their partition data (for this partition), then combining them and storing in the tablestore
case class PreparePartition(dataSource : DataSource) extends QueryPlanItem
