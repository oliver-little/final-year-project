package org.oliverlittle.clusterprocess.util

import org.oliverlittle.clusterprocess.UnitSpec

class LRUCacheTest extends UnitSpec {
    "An LRUCache" should "add an item to the cache" in {
        var lruCache = LRUCache[Int]()
        lruCache = lruCache.add(1)
        lruCache.order should be (Seq(1))
    }

    it should "add a sequence of items to the cache" in {
        var lruCache = LRUCache[Int]()
        lruCache = lruCache.addAll(Seq(3, 1, 2))
        lruCache.order should be (Seq(3, 1, 2))
    }

    it should "move an item in the list to the front of the cache" in {
        var lruCache = LRUCache[Int]()
        lruCache = lruCache.addAll(Seq(3, 1, 2))
        lruCache = lruCache.access(2)
        lruCache.order should be (Seq(2, 3, 1))
    }

    it should "do nothing if accessing an item not in the cache" in {
        var lruCache = LRUCache[Int](Seq(3, 1, 2))
        lruCache = lruCache.access(4)
        lruCache.order should be (Seq(3, 1, 2))
    }

    it should "move a list of items to the front of the cache" in {
        var lruCache = LRUCache[Int](Seq(3, 1, 2))
        lruCache = lruCache.accessAll(Seq(2, 1))
        lruCache.order should be (Seq(2, 1, 3))
    }

    it should "delete an item from the cache" in {
        var lruCache = LRUCache[Int](Seq(3, 1, 2))
        lruCache = lruCache.delete(1)
        lruCache.order should be (Seq(3, 2))
    }

    it should "delete a list of items from the cache" in {
        var lruCache = LRUCache[Int](Seq(3, 1, 2))
        lruCache = lruCache.deleteAll(Set(1, 3))
        lruCache.order should be (Seq(2))
    }
    
    it should "get the least recently used item" in {
        var lruCache = LRUCache[Int](Seq(3, 1, 2))
        lruCache.getLeastRecentlyUsed should be (Some(2))
    }

    it should "return None for least recently used if there are no items" in {
        var lruCache = LRUCache[Int]()
        lruCache.getLeastRecentlyUsed should be (None)
    }
}