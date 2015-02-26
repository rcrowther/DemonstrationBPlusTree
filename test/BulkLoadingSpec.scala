package tree

import org.scalatest._

import java.io.File
import java.nio.file.Path


// TODO: This test doesnt cover a symlink to a file
class BulkLoadingSpec
    extends FunSpec
    with Fixture
{
  describe("A DemonstrationBPlusTree"){

    it("should bulkload an element, then be size 1") {
      val t = DemonstrationBPlusTree.bulkload(2, 2, Seq(
        data(0)
      ))
      assert(t.size === 1)
    }

    it("should bulkload a few elements, then be size 3") {
      val t = DemonstrationBPlusTree.bulkload(2, 2, data.slice(0, 3))
      assert(t.size === 3)
    }

    it("should bulkload several forward linear keyed elements (enough for splits), then be size 26") {
      val t = DemonstrationBPlusTree.bulkload(2, 2, data)
//println(t.toFrameString())
      assert(t.size === 26)
    }

    it("should bulkload several backward linear keyed elements, then be size 22") {
      val t = DemonstrationBPlusTree.bulkload(2, 2, reverseData)
      assert(t.size === 22)
    }

  }//describe

}//BulkLoadingSpec
