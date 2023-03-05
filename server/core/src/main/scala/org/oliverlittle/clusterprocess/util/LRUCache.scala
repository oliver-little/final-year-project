package org.oliverlittle.clusterprocess.util

case class LRUCache[T](order : Seq[T] = Seq()):
    def add(e : T) : LRUCache[T] = 
        copy(order=e +: order)

    def access(e : T) : LRUCache[T] = 
        // Search the list until we find our element
        order.span(_ != e) match {
            // Before is all elements before ours, head is our element, after is everything else
            // Then reorder it so our element
            case (before, head :: after) => copy(order=head +: (before ++ after))
            case _ => this
        }

    def delete(e : T) : LRUCache[T] = copy(order=order.filter(_ == e))

    def deleteAll(items : Set[T]) : LRUCache[T] = copy(order=order.filter(e => items.contains(e)))

    def getLeastRecentlyUsed : Option[T] = order.lastOption