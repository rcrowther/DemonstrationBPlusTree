package tree

import org.scalatest._

import java.io.File
import java.nio.file.Path


// TODO: This test doesnt cover a symlink to a file
class SizingSpec
    extends FunSpec
    with Fixture
{
  describe("A DemonstrationBPlusTree"){
    val t = bPlusTree

    it("should load an element, then be size 1") {
      t += data(1)
      assert(t.size === 1)
    }

    it("should clear(), then be size 0") {
      t.clear
      assert(t.size === 0)
    }

    it("should refuse to load element with simialar keys, then be size 1") {
      t += data(1)
      t += data(1)
      assert(t.size === 1)
    }

    it("should load less than a leaf elements (3), then be size 3") {
      t.clear()
      t += data(1)
      t += data(2)
      t += data(3)
      assert(t.size === 3)
    }

    it("should load several forward linear keyed elements (enough for splits), then be size 26") {
      t.clear()
      data.foreach(t += _)
      assert(t.size === 26)
    }

    it("should load several backward linear keyed elements (causing massive splits), then be size 22") {
      t.clear()
      reverseData.foreach(t += _)
      assert(t.size === 22)
    }

  }//describe

}//SizingSpec
