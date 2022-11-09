package org.oliverlittle.clusterprocess.model.field.expressions.implicits

import java.time.OffsetDateTime

import org.oliverlittle.clusterprocess.model.field.expressions.{V}

implicit def stringToV(s : String) : V = new V(s)
implicit def intToV(i : Int) : V = new V(i.toLong)
implicit def longToV(l : Long) : V = new V(l)
implicit def floatToV(f : Float) : V = new V(f.toDouble)
implicit def doubleToV(d : Double) : V = new V(d)
implicit def offsetDateTimeToV(l : OffsetDateTime) : V = new V(l)
implicit def boolToV(b : Boolean) : V = new V(b)