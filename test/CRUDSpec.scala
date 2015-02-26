package tree

import org.scalatest._

import java.io.File
import java.nio.file.Path


// TODO: This test doesnt cover a symlink to a file
class CRUDSpec
    extends FunSpec
    with Fixture
{
  describe("A DemonstrationBPlusTree"){
    val t = bPlusTree

    it("should load 12 elements, then be size 12") {
      data.slice(0, 12).foreach (t += _)
      assert(t.size === 12)
    }

    it("element 9 (using get()) should be 'Barbara Ann'") {
      assert(t.get(9).get === "Barbara Ann")
    }

    it("after using update(), element 9 (using get()) should be 'Help Me, Rhonda'") {
      t.update(9, "Help Me, Rhonda")
      assert(t.get(9).get === "Help Me, Rhonda")
    }

    it("after deleting three elements, size should be 9") {
      t --= Seq(4, 7, 12)
      assert(t.size === 9)
    }

    it("should delete down to zero with a tree of three elements (can cause problems!)") {
      t.clear()
      data.slice(0, 3).foreach (t += _)
      t -= 1
      t -= 2
      t -= 3
      assert(t.size === 0)
    }

    it("should robustly refuse to delete from an empty tree") {

      t -= 4
      assert(t.size === 0)
    }

  }//describe

}//CRUDSpec
