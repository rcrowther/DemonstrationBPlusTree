package tree

import org.scalatest._

import java.io.File
import java.nio.file.Path


// This test is only valid for a tree with branch factor 4.
class InternalProvocationSpec
    extends FunSpec
    with Fixture
{
  describe("A DemonstrationBPlusTree, when triggered in specific ways,"){
    val t = bPlusTree

    // Coalese
    // on a factor four tree, four items splits for two nodes, and
    // splits root. So end deletion should trigger coalescing
    it("should coalesce leaves to the left on deletion") {
      t.clear
      data.slice(0, 4).foreach(t += _)
      t -= 4
      assert(t.size === 3)
    }

    // Leftmost removal forcing swap algorithm
    // First create forks, then remove from the the right fork so it
    // is underpopulated, then try remove from a low key to the left,
    // thus asking for coalescense from the right.
    it("should coalesce leaves to the right on low index deletion") {
      t.clear
      data.slice(0, 5).foreach(t += _)
      t --= Seq(5, 2)
      assert(t.size === 3)
    }

    // leaf right redistribute
    // adding 3,4,5 will cause the second node to max out. Removing
    // very low index 2 (or 1) makes the first node too small. With a
    // maxed node to the right, nothing to the left, it must
    // redistribute from the right.
    it("should redistribute leaves to the right on deletion") {
      t.clear
      data.slice(0, 5).foreach(t += _)
      println(t.toFrameString())
      t -= 1
      assert(t.size === 4)
    }

    // leaf left redistribute
    // Adding 3, after 8 and 9 have spilt, will make node 1
    // full. Deleting 9 will cause an attempt to get the too-small
    // node to redistribute from the full node (not coalese)
    it("should redistribute leaves to the left on deletion") {
      t.clear
      //NB: The keys are one in front of the indexes!
      t += data(0)
      t += data(1)
      t += data(7)
      t += data(8)
      t += data(2)

      t -= 9
      assert(t.size === 4)
    }

    // fork from right redistribute
    // adding 6-13 will cause the third fork to max out. Removing
    // anything from the first fork (1-4), will cause it to coalese
    // with the second fork into one leaf.  With a maxed fork to the
    // right, nothing to the left, the fork must must redistribute
    // from the right.
    it("should redistribute leaves from the right on deletion") {
      t.clear
      t.clear()
      data.slice(0, 12).foreach(t += _)
      t -= 3
      assert(t.size === 11)
    }

    // fork from left redistribute
    // Adding 5/6/7/8, after 1-4 and 10-15 have established two forks,
    // will max out the first subtree (with leaves).  Deleting
    // 15/14/13 will cause the second fork to coalese smaller until
    // too small, when it will attempt to redistribute from a full
    // node (it can not coalese, the first fork is full). Since fork
    // two is the lask fork, it must take data from the left.
    it("should redistribute leaves from the left on deletion") {
      t.clear
      data.slice(0, 4).foreach(t += _)
      data.slice(9, 15).foreach(t += _)
      data.slice(4, 8).foreach(t += _)
      //println(t.toFrameString)
      t --= Seq(15, 14, 13)
      //println(t.toFrameString)
      assert(t.size === 11)
    }

  }//describe

}//InternalProvocationSpec
