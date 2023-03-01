package org.oliverlittle.clusterprocess

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{Inside, OptionValues, AppendedClues}
import org.scalatest.matchers._

abstract class UnitSpec extends AnyFlatSpec with Inside with OptionValues with AppendedClues with should.Matchers