package org.oliverlittle.clusterprocess.util

import org.oliverlittle.clusterprocess.UnitSpec

class PartialFoldTest extends UnitSpec {
    "A PartialFold" should "apply a generic partial fold correctly" in {
        PartialFold.partialFold(Seq("e", "e", "e", "e", "e"), l => l.nonEmpty && l.head.endsWith("e"), (l, i) => l.head ++ i :: l.tail) should be (Seq("eeeee"))
        PartialFold.partialFold(Seq("h", "e", "l", "l", "o"), l => l.nonEmpty && l.head.endsWith("e"), (l, i) => l.head ++ i :: l.tail) should be (Seq("h", "el", "l", "o"))
        PartialFold.partialFold(Seq("h", "o", "l", "l", "o"), l => l.nonEmpty && l.head.endsWith("e"), (l, i) => l.head ++ i :: l.tail) should be (Seq("h", "o", "l", "l", "o"))
    }
}