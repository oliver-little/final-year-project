package org.oliverlittle.clusterprocess

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.{Inside, OptionValues, AppendedClues}
import org.scalatest.matchers._

abstract class AsyncUnitSpec extends AsyncFlatSpec with Inside with OptionValues with AppendedClues with should.Matchers