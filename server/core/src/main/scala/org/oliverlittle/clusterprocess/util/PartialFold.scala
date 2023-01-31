package org.oliverlittle.clusterprocess.util

object PartialFold:
    /**
	 *  Generic helper function, abstracted out for testing
	 *  Adapted from:
	 *  https://stackoverflow.com/questions/28423154/how-can-i-implement-partial-reduce-in-scala
	 */
	def partialFold[T](input : Seq[T], condition : Seq[T] => Boolean, combiner : (List[T], T) => List[T]) : Seq[T] = input.foldLeft(List[T]()) {
		(rs, s) => 
			// Check the condition
			if condition(rs) then
				// If true, combine
				combiner(rs, s)
			else 
				s :: rs
	}.reverse