// Copyright (C) 2015 Robert Crowther
//
// The DemonstrationBPlusTree is free software: you can redistribute
// it and/or modify it under the terms of the GNU General Public
// License as published by the Free Software Foundation, either
// version 3 of the License, or (at your option) any later version.
//
// The DemonstrationBPlusTree is distributed in the hope that it will
// be useful, but WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
// PURPOSE. See the GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with ssc.  If not, see <http://www.gnu.org/licenses/>.
//
package tree


import scala.reflect.{ClassTag, classTag}


/** Demonstration implementation of a B+Tree.
  * 
  * With Create/Read/Update/Delete methods, and initial bulk-loading.
  * 
  * The tree was originally translated from,
  * [[http://www.amittai.com/prose/bpt.c]].
  * Not much remains besides some notes? Some designs have been
  * altered.
  * 
  * The code referenced above is annotated as
  * 
  * ''"Following Silberschatz, Korth, Sidarshan, Database Concepts, 5th ed."''
  * 
  * but I am unable to verify this.
  *
  * These were useful, also, 
  *
  * [[http://classes.soe.ucsc.edu/cmps180/Winter03/Lectures/B+-Trees.pdf]]
  *
  * @define coll tree
  * 
  * @tparam A the keys
  * @tparam B the values
  *
  * @param minChildren Minimum number of children in an internal
  * (fork) node. The max ("branchingFactor/order") is set to 2*minChildren
  */
// TODO: Make sure k is renamed key, especially in parameters
//TODO: Add a ++=
// The 'implicit val ordering...' declaration is a Scala way of
// deciding what goes before what in keys. Non-Scala users
// have no need to understand this, except that it means
// ANY value supplied to this generic construction can
// be compared using operators like '<' and '>'.
// Constructions in other languages may need to provide code
// to compare keys, and insert this at appropriate points.
class DemonstrationBPlusTree[A: ClassTag, B: ClassTag](private val minChildren: Int)(implicit val ordering: Ordering[A]) {

  if (minChildren < 2) throw new Exception("minChildren must be >= 2")
  
  // Branching factor is the number of children allowed in fork
  // (internal) nodes.  The actual number of children is supposed to
  // vary max/2 <= childSize <= branching factor This allows for odd
  // numbers in the branching factor. However, this implementation
  // uses a small trick, it asks for the minimum number of children
  // then doubles it. This makes the branch factor always even.
  // Consistently even branch factors simplify code a lot, and do not
  // restrict practical usage of a B+ Tree.
  
  // Note that, in leaves, the number of children/records/vals is the
  // same as the number of keys, so the key values can be used in
  // those positions.
  
  // Used for lower limits except root.
  // Needs to round up.
  private val branchingFactor : Int = minChildren << 1
  
  // In fork, max children is defined by branch factor. Includes root
  // = factor
  private val forkMaxChildren: Int = branchingFactor

  // In forks (excepting root), min children is half branch factor
  // Root is excepted as it can go down to two children. This
  // implementation also embraces a null state (the root as an empty
  // leaf)
  // = factor/2
  private val forkMinChildren: Int = minChildren

  // In forks, min keys is one less than min allowed children.
  // = factor/2 - 1
  private val forkMinKeys: Int = forkMinChildren - 1

  // In all nodes, max keys is one less than max allowed children.
  // This is the same value for roots and leaves.
  // = factor - 1
  private val nodeMaxKeys: Int = branchingFactor - 1
  
  // In leaves, minimum entries is the same as fork (all node bar
  // root) children forkMinChildren could be used, but here for
  // illustration.
  // = factor/2
  private val leafMinKeys: Int = minChildren

  
  /** A node in the b+ tree.
    * 
    * Two main types of nodes are required, one for leaves (with
    * pointers to some data) and one for internal nodes (with pointers
    * to other nodes). The typing can be handled in several ways -
    * different nodes, AnyRef typing, or the solution used here, a
    * node carrying fields of both types, one of which goes unused.
    * 
    * In an internal node, the first child refers to a sub-tree with
    * keys less than the first key in the keys array. This continues:
    * key(i) refers to child(i) carrying a tree with
    * keys less than the indexed key.  A conclusion, implemented
    * several times, is that, for key(i), child(i + 1) points to a
    * tree with keys greater than or equal to key(i).  This is an
    * important feature to understand about B+ trees, that pointers to
    * data matching keys (for equality) are found in the following
    * (children(i + 1)) child tree (not the directly indexed tree
    * children(i)).
    * 
    * A leaf is much simpler 1 to 1 relationship, key(i) referring to
    * vals(i).  One point to note is that the vals array never fills
    * to it's maximum. It's maximum size is Max - 1. The reason for
    * this is that data additions to near-full leaf nodes can be
    * tacked on the end, then the algorithm for leaf node splitting
    * can be a very simple halving of the data, with the newly added
    * key-value already loaded in a correctly key-ordered position.
    * 
    * Note that all nodes in this implementation have arrays one element longer
    * than will be found in standard documentation. That is not because
    * a linking pointer has been added to the leaves (the links are very obviously separate).
    * It is because this implementation of a B+Tree reuses the arrays
    * for making splits. Thus the extra keys and data must go somewhere,
    * and they go on the end of existing arrays.
    * 
    * @param parent refers to a parent node. Used in...?
    * @param isLeaf true if this node is a leaf, false otherwise.
    * @param nextLeaf joins leaf nodes together so they can be
    * traversed as a list.
    */
  private class Node[A, B] (
    var parent: Option[Node[A, B]],
    var keys: Array[A],
    // was pointers, for branch forks
    var children: Array[Node[A, B]],
    // was pointers, for leaves
    var vals : Array[B],
    var isLeaf: Boolean,
    var nextLeaf: Option[Node[A, B]]
  )(implicit m1: ClassTag[A], m2: ClassTag[B])
  {
    // Note. Size being number of keys, not children
    var num_keys: Int = 0
  }//Node


  /** Creates a new leaf with one key value.
    */
  private def mkLeaf(k: A, v : B)
      : Node[A, B] =
  {
    val keys = classTag[A].newArray(len = branchingFactor)

    // The BTree nodes must be provided with some kind of empty sequential containers.
    // The above classTag expression/thing (Scala way of generating a Java-compatible generic array)
    // is a way of generating a space-allocated, content-empty array.
    // (Scala avoids 'null' in favour of the Option class.
    // However, to play nice with Java, it will fill out the
    // empty array with 'null's)
    // In many languages such as C++, some variant on 'null' or simple space-allocation would be used.
    // Please consider that these node containers will be accessed
    // without further content allocation, and so accessing code must be capable
    // of handling the initial constructed state.
    // If a Scala version of this class is to be used, but with the types specified,
    // either these containers should be sequences (and thus part of the collection system),
    // or should be filled with some neutral value. The lines commented out below fill
    // the arrays with some initial value declared and set in the complementary object.
    // (they are of only passing interest to non- Scala users)
    //val keys = Array.fill[String](branchingFactor)(BPlusTree.emptyKey)

    keys(0) = k

    val leaves =  classTag[B].newArray(len = branchingFactor)
    //val leaves = Array.fill[Int](branchingFactor)(0)

    leaves(0) = v
    
    val leaf = new Node[A, B] (
      None,
      keys,
      Array.empty[Node[A, B]],
      leaves,
      true,
      None
    )
    leaf.num_keys = 1
    leaf
  }
  
  /** Creates a new empty leaf.
    */
  private def mkLeaf()
      : Node[A, B] =
  {
    val keys = classTag[A].newArray(len = branchingFactor)
    val leaves =  classTag[B].newArray(len = branchingFactor)

    val leaf = new Node[A, B] (
      None,
      keys,
      Array.empty[Node[A, B]],
      leaves,
      true,
      None
    )
    leaf.num_keys = 0
    leaf
  }
  
  /** Create a leaf with full data.
    * 
    * Used in bulk loading.
    */
  private def leafBulkLoad(
    keys : Seq[A],
    leaves : Seq[B]
  )
      : Node[A, B] =
  {
    
    val leaf = new Node[A, B] (
      None,
      keys.toArray.padTo(branchingFactor, null).asInstanceOf[Array[A]],
      Array.empty[Node[A, B]],
      leaves.toArray.padTo(branchingFactor, null).asInstanceOf[Array[B]],
      true,
      None
    )

    leaf
  }
  
  /** Creates a new empty fork.
    */
  private  def mkFork()
      : Node[A, B] =
  {
    val keys = classTag[A].newArray(len = branchingFactor)

    val children = new Array[Node[A, B]](branchingFactor + 1)

    new Node[A, B](
      None,
      keys,
      children,
      Array.empty[B],
      false,
      None
    )
  }
  
  /** Creates a new fork with one key and children.
    */
  private  def mkRoot(left: Node[A, B], k: A, right: Node[A, B])
      : Node[A, B] =
  {
    val keys = classTag[A].newArray(len = branchingFactor)

    val children = new Array[Node[A, B]](branchingFactor + 1)
    keys(0) = k
    children(0) = left
    children(1) = right

    val root = new Node[A, B] (
      None,
      keys,
      children,
      Array.empty[B],
      false,
      None
    )
    root.num_keys = 1
    root
  }



  /** Root of the tree built in this object.
    */
  private var treeRoot: Node[A, B] = mkLeaf()

  
  /** The size of this $coll.
    */
  var size: Int = 0
  
  ////////////////////////////////////

  
  // Auxillery Methods //
  // Methods generalizing common array handling methods.
  
  /** Finds index of first occurrence of some value in this mutable sequence.
    *  
    * Don't use negative numeric keys - this is rather crude,
    * and geared towards the tree.
    * 
    * @return the index the key was found at, or -1 if not found.
    */
  def fastIndexOf[G](a: Array[G], v: G)(implicit ordering: Ordering[G])
      : Int =
  {
    var end = a.size - 1
    var start = 0
    var middle = end >> 1
    var cont = true
    // catch a low start, it flips the middle,
    // even sending it negative.
    // high is ok
    if (ordering.lt(v, a(0))) -1
    else {
      //var current = middle
      while(!ordering.equiv(v, a(middle)) && cont) {
        //println(s"end $end start $start")
        if(ordering.gt(v, a(middle))) start = middle + 1
        else end = middle - 1
        middle = ((end - start) >> 1) + start
        //println(s"  2 end $end start $start")
        cont = (start <= end)
      }
      if (ordering.equiv(v, a(middle))) middle
      else -1
    }
  }
  
  /** Copies values from this array to another.
    *  
    * @param src the array to be copied from
    * @param from the first index of copying
    * @param until the index succeeding the last index of copying
    * @param dst the array to be copied to
    * @param the offset in the `dst` array to start inserting values 
    * @return the index after the last insert in the destination 
    */
  private def arraycopy[A](src: Array[A], from: Int, until: Int, dst: Array[A], offset: Int)
      : Int =
  {
    var i = from
    var j = offset
    while (i < until) {
      dst(j) = src(i)
      i += 1
      j += 1
    }
    j
  }
  
  /** Copies values from two arrays to two other arrays.
    *  
    * @param src1 the first array to be copied from
    * @param src2 the second array to be copied from
    * @param from the first index of copying
    * @param until the index succeeding the last index of copying
    * @param dst1 the first array to be copied to
    * @param dst2 the second array to be copied to
    * @param the offset in the `dst` arrays to start inserting values 
    * @return the index after the last insert in the destination 
    */
  private def dualArrayCopy[A, B](
    src1: Array[A],
    src2: Array[B],
    from: Int,
    until: Int,
    dst1: Array[A],
    dst2: Array[B],
    offset: Int
  )
      : Int =
  {
    var i = from
    var j = offset
    while (i < until) {
      dst1(j) = src1(i)
      dst2(j) = src2(i)
      i += 1
      j += 1
    }
    j
  }
  
  /** Move the elements of an array right
    *  
    * {{{ 
    * val a = Array(1, 2, 3)
    * arrayShuntRight(a, 0, a.size)
    * a
    * > Array(1, 1, 2)
    * }}} 
    * 
    * @param src the array to be shifted
    */
  private def arrayShuntRight[A](src: Array[A]) {
    var i = src.size - 1
    
    while (i > 0) {
      src(i) = src(i - 1)
      i -= 1
    }
  }

  /** Move the elements of two arrays right
    * {{{ 
    * val a = Array(1, 2, 3)
    * val b = Array(5, 6, 7)
    * 
    * dualArrayShuntRight(a, b, 0, a.size)
    * a
    * > Array(1, 1, 2)
    * b
    * > Array(5, 5, 6)
    * }}}
    * 
    * @param src1 the first array to be shifted
    * @param src2 the second array to be shifted
    */
  private def dualArrayShuntRight[A, B](
    src1: Array[A],
    src2: Array[B]
  ) {
    var i = src1.size - 1
    
    while (i > 0) {
      src1(i) = src1(i - 1)
      src2(i) = src2(i - 1)
      i -= 1
    }
  }
  
  /** Move a slice of array elements right
    *  
    * The element at `from` is duplicated to the right. The element at
    * `to` is replaced with the element to the left.
    * 
    * {{{ 
    * val a = Array(1, 2, 3, 4, 5)
    * arrayShuntRight(a, 1, 3)
    * a
    * > Array(1, 2, 2, 3, 5)
    * }}} 
    *  
    * @param src the array to be shifted
    * @param from index of the first element to be moved
    * @param until the index of the element after the last to be moved 
    */
  private def arrayShuntRight[A](src: Array[A], from: Int, to: Int) {
    var i = to
    
    while (i > from) {
      src(i) = src(i - 1)
      i -= 1
    }
  }

  
  /** Move array contents right along two arrays
    * 
    * The elements at `from` are duplicated to the right. The elements
    * at `to` are replaced with the elements to the left.
    * 
    * {{{ 
    * val a = Array(1, 2, 3, 4, 5)
    * val b = Array(5, 6, 7, 8, 9)
    * 
    * dualArrayShuntRight(a, 1, 3)
    * a
    * > Array(1, 2, 2, 3, 5)
    * b
    * > Array(5, 6, 6, 7, 9)
    * }}} 
    * 
    * 
    * @param src1 the first array to be shifted
    * @param src2 the second array to be shifted
    * @param from index of the first element to be moved
    * @param until the item after the last to be moved 
    */
  private def dualArrayShuntRight[A, B](
    src1: Array[A],
    src2: Array[B],
    from: Int,
    to: Int
  ) {
    var i = to
    //println(s"i: $i")
    while (i > from) {
      src1(i) = src1(i - 1)
      src2(i) = src2(i - 1)
      i -= 1
    }
  }
  
  /** Move array contents left along the array
    * 
    * The element at `from` is replaced by the element to the
    * right. The element before `to` is duplicated to the left. To
    * shift an array left,
    * 
    * {{{ 
    * val a = Array(1, 2, 3)
    * arrayShuntLeftUntil(a, 0, a.size)
    * a
    * > Array(2, 3, 3)
    * }}} 
    * 
    * @param src the array to be shifted
    * @param from index of the first element. The element next to
    * this will be moved over this, and this one destroyed
    * @param until the index of the element after the last to be
    * moved. Elements at `until' will not be touched, but the
    * previous element will be shifted left, duplicating it. 
    */
  private def arrayShuntLeftUntil[A](src: Array[A], from: Int, until: Int) {
    var i = from + 1
    
    while (i < until) {
      src(i - 1) = src(i)
      i += 1
    }
  }
  
  /** Move array contents left along two arrays
    * 
    * The elements at `from` are replaced by the elements to the
    * right. The elements before `to` are duplicated to the left. To
    * shift two arrays left,
    * 
    * {{{ 
    * val a = Array(1, 2, 3)
    * val b = Array(5, 6, 7)
    * 
    * dualarrayShuntLeftUntil(a, b, 0, a.size)
    * a
    * > Array(2, 3, 3)
    * b
    * > Array(6, 7, 7)
    * }}} 
    * 
    * @param src1 the first array to be shifted
    * @param src2 the second array to be shifted
    * @param from index of the first element to be moved
    * @param until the item after the last to be moved 
    */
  private def dualarrayShuntLeftUntil[A, B](
    src1: Array[A],
    src2: Array[B],
    from: Int,
    until: Int
  )
  {
    var i = from + 1
    
    while (i < until) {
      src1(i - 1) = src1(i)
      src2(i - 1) = src2(i)
      i += 1
    }
  }
  
  //////////
  
  // Bulkloading methodology //

  /** Returns the left sibling of a node
    *  
    * Searches the tree by recursing to root, if necessary, through
    * fork or leaf nodes, looking for a child with a sibling to the
    * left.  If it finds one, it recurses back down from the found
    * sibling, tracking strictly to the right.
    * 
    * The search has a (deliberate) lack of error checking, it ignores
    * keys.
    */
  private def treeLeftSibling(node: Node[A, B])
      : Node[A, B] =
  {
    println(s"  treeLeftSiblingIndex ${node.num_keys}")
    val parentO = node.parent
    val parent =
      if (parentO == None) {
        throw new Exception("Error, searching for sibling but found root")
      }
      else {
        parentO.get
      }
    var i = 0
    while((i <= parent.num_keys) && parent.children(i) != node) {
      i += 1
    }

    if (parent.children(i) == node && i > 1) {
      // A fork node with a valid left branch
      // Huzzah! chase down to the leaf
      var chaseNode = parent.children(i - 1)
      while(!chaseNode.isLeaf) chaseNode = chaseNode.children(chaseNode.num_keys)
      chaseNode
    }
    else treeLeftSibling(parent)
  }
  

  
  /** Given a fork, creates a new (right) sibling.
    *  
    * @param node a full node
    * @param first key for the new node
    */
  private def forkCreateSibling(node: Node[A, B], key: A)
      : Node[A, B] =
  {
    val parentO = node.parent

    if (parentO == None) {
      // The node is root. Split with a new fork
      //println(s"    forkCreateSibling, root rebuild k:$key")

      val oldFork = treeRoot
      treeRoot = mkFork()
      treeRoot.keys(0) = key
      treeRoot.children(0) = oldFork
      treeRoot.num_keys = 1
      oldFork.parent = Some(treeRoot)
      val newFork = mkFork()
      treeRoot.children(1) = newFork
      newFork.parent = Some(treeRoot)
      newFork
    }
    else {
      val parent = parentO.get
      if (parent.num_keys < nodeMaxKeys) {
        //println(s"    forkCreateSibling, adding... key: $key")

        // add a new sibling. easy...
        val newFork = mkFork()
        val numKeys = parent.num_keys
        parent.keys(numKeys) = key
        parent.children(numKeys + 1) = newFork
        parent.num_keys += 1
        newFork.parent = Some(parent)
        newFork
      }
      else {
        //println(s"    forkCreateSibling, recursing")

        val newParent = forkCreateSibling(parent, key)
        val newFork = mkFork()
        newParent.children(0) = newFork
        // No need to increase keys, we have none yet and they
        // should be in a parent
        newFork.parent = Some(newParent)
        newFork
      }
    }
  }
  
  
  /** Appends a bulk load created leaf to the tree.
    *  
    * Does all necessary property setting, link joining, and node
    * splitting.
    */
  private def leafBulkLoad(leaf: Node[A, B]) {
    //Get rightmost leaf
    var chaseNode = treeRoot
    while (!chaseNode.isLeaf) chaseNode = chaseNode.children(chaseNode.num_keys)
    
    //println(s"  leafBulkLoad")
    //println(toFrameString())

    // Parent can be accessed immediately.
    // Even in the case of the first leaf the tree was preset to have
    // one leaf on top of a fork.
    val parent = chaseNode.parent.get

    
    //val parentForLeaf: Node[A, B] =
    if (parent.num_keys < nodeMaxKeys)  {
      //println(s"  leafBulkLoad, into fork")

      // the new leaf can fit into the fork
      var keyCount = parent.num_keys
      // The parent already has an initial child
      // so the key for the previous child is this leaf first key
      parent.keys(keyCount) = leaf.keys(0)
      // and add this leaf (key to arrive later, if another leaf added)
      parent.children(keyCount + 1) = leaf
      // Link the leaves
      parent.children(keyCount).nextLeaf = Some(leaf)
      // set the parent
      leaf.parent = Some(parent)
      // increment the keycount
      parent.num_keys += 1
    }
    else {
      //println(s"  leafBulkLoad, split key ${leaf.keys(0)}")
      // a new fork is needed
      // the key splitting from the old fork will be a duplicate of
      // the first key from the leaf
      val newParentSibling = forkCreateSibling(parent, leaf.keys(0))
      // new fork arrived,
      // no need for a key (the key went into the parent), place the child
      newParentSibling.children(0) = leaf
      // Link the leaves. Original parent, last child, to the new leaf
      parent.children(parent.num_keys).nextLeaf = Some(leaf)
      // set the leaf parent
      leaf.parent = Some(newParentSibling)
      // no need to increment the keycount
    }
  }
  
  
  /** Preload an empty tree with data.
    *  
    * Should be a faster than multiple appends.  
    * 
    * @param density number of given items per leaf. Must be
    * leafMinKeys => density <= nodeMaxKeys
    */
  // This version may be slower at times due to the Scala
  // manipulations (and flexibility), but the principle is ok.
  //
  // density is an interesting parameter. A tree can be loaded with
  // all leaves packed to the maximum (nodeMaxKeys) which results in
  // the most compact tree. However, this may not be the best option,
  // as new inserts into any place other than the end will cause
  // immediate splitting. So some figure less than the max, to allow
  // some looseness in the tree, may be preferable.
  private def bulkLoad(density: Int, xs: TraversableOnce[(A, B)])
  {
    // bulk loading will only work on an empty tree
    if (size != 0) {
      throw new Exception("tree size must be 0 for bulk loading")
    }
    
    if (density < leafMinKeys || density > nodeMaxKeys) {
      throw new Exception(s"bulkloading density $density is outside usable range. Density for bulk loading must be >= $leafMinKeys (leafMinKeys) && <= $nodeMaxKeys (nodeMaxKeys)")
    }
    
    // Form an iterator of nodeMaxKey size groups
    // Will require much more work in non-Scala languages
    // but the below outlines the plan
    val data = xs.toStream.sortWith((a, b) => ordering.lt(a._1, b._1) )
    val it = data.iterator.grouped(density)

    if(it.hasNext) {
      // ok, we have some input

      // The tree root should be a leaf, as the tree is empty
      // but we don't bother. If there is input to be loaded,
      // we need a fork node, not a leaf.
      treeRoot = mkFork()

      // The first leaf is special. There is no key to it in the root fork
      val (keys: Seq[A], leaves: Seq[B]) = it.next().unzip

      val leaf = leafBulkLoad(
        keys.toArray.padTo(branchingFactor, null).asInstanceOf[Array[A]],
        leaves.toArray.padTo(branchingFactor + 1, null).asInstanceOf[Array[B]]
      )
      size += keys.size
      leaf.num_keys = keys.size

      // place this into the root. No key.
      treeRoot.children(0) = leaf
      leaf.parent = Some(treeRoot)
      
      // At which point, we have a correctly structured tree, with one
      // leaf (with an incorrect one child) on top of one fork.
      // Note: it has no keys (key_num == 0) but following code should
      // work ok.
      
      // Deal with the remaining input
      while(it.hasNext) {
        val (keys: Seq[A], leaves: Seq[B]) = it.next().unzip

        val leaf = leafBulkLoad(
          keys,
          leaves
        )
        size += keys.size
        leaf.num_keys = keys.size
        leafBulkLoad(leaf)
      }

      // So the root had a guaranteed fork node, the empty tree was
      // preconstructed into a fork containing a leaf. This reduces
      // cruft in the bulk-loading method.
      //
      // However, this means if only one leaf was loaded, the tree is
      // not only unbalanced, but a corrupt structure (empty node, one
      // child - leaf).  This can be easily fixed by running
      // rebalanceRoot(). If the structure is bigger, then rebalance
      // root does nothing (it checks for an empty root fork with a
      // leaf child, a situation which can arise after deletion, and
      // describes our situation exactly).
      rebalanceRoot()
      
      
      // The last leaf node may be undersized, as it is the remains of
      // the grouping.  It will need rebalancing. Also, the method of
      // placing leaves forcibly in blocks into forks, can cause a
      // problem similar to the root problem, where one child is
      // orphaned in a fork.
      //
      // Unfortunately, unlike rebalanceRoot(), rebalanceNode() can
      // not handle this scenario.  It will search for a node to it's
      // immediate left (doesn't exist), when it doesn't exist assume
      // it can swap with a node to it's right. In an uncorrupt tree
      // (even if unbalanced) these nodes would exist (an orphaned
      // child node would have been coalesced into another node).  On
      // finding a lonely child, the rebalanceRoot() method will
      // crash.
      //
      // There are many solutions to this - rewrite rebalanceNode()
      // with deeper alternative and protection code, running
      // specialist parts of the balance methodology, a specialist
      // bulkTreeTailRebalance method, all with pros and cons. Note
      // that cheap fixes will not work. For example, coalesce() can
      // not be reliably called directly because the caller may have
      // requested a fully packed tree (so, no space to coalesce to).
      //
      // The solution here is to fix the tree so it is not corrupt -
      // unbalanced maybe, but not corrupt. The solution depends on
      // the fact this version of bulkloading fills forks to the
      // max. It would work as long as a bulkloading method overfills
      // forks with leaves, even if not to the max (but it will fail
      // if minimum fork filling was allowed). The solution searches
      // for left sibling leaves to the lonely leaf through the whole
      // tree, not only the node.  So it will find leaf nodes in a
      // left fork. Since any previous fork has leaves to spare
      // (because they are not minimum filled), the method then takes
      // one leaf, only one, from the sibling fork, and places it in
      // the lone leaf fork node. Now the tree is uncorrupt, if
      // possibly unbalanced, so rebalanceNode() run safely.
      //
      // This is not the optimal method - a custom redistribute would
      // be. But it is a reuse of existing concepts and code, and is
      // efficient as the other code.
      
      // Running a rebalance on the final node should restore BTree
      // conditions.

      // Find the end leaf node
      // (as an alternative, could be stashed from above)
      var chaseNode = treeRoot
      while (!chaseNode.isLeaf) chaseNode = chaseNode.children(chaseNode.num_keys)
      
      // From the discussion above about chasing left nodes through the whole tree,
      // this fix must be protected against running on a sole leaf.
      // Expression precedence will protect against null exceptions.
      // Also, the code is only significant when used on a sole child.
      // Moreover, it should be avoided if the end fork is full (it
      // would try to forcibly overfill it)
      // So the protection looks like this...
      if (chaseNode != treeRoot && chaseNode.parent.get.num_keys == 0) {
        println("Darn it. One-child last fork!")
        val sibling = treeLeftSibling(chaseNode)
        println(toStringNode(sibling))
        //bulkLoadRedistribute(chaseNode, leftSibling)
        println(s"one child last node? ${chaseNode.num_keys}")

        val parent = chaseNode.parent.get
        val siblingParent = sibling.parent.get
        // Shunt the lonely child leaf up one
        parent.children(1) = parent.children(0)
        // grab the last sibling leaf-child and bring it over
        parent.children(0) = siblingParent.children(siblingParent.num_keys)
        println(s"siblingParent.num_keys ${siblingParent.num_keys}")
        siblingParent.num_keys -= 1
        // say, hey, you have a new parent
        parent.children(0).parent = Some(parent)
        // The parent's sole key is the now not-so-lonely child's first entry
        parent.keys(0) = parent.children(1).keys(0)

        //println(s"siblingParent.num_keys ${sibling.parent.get.num_keys}")

        parent.num_keys = 1
      }

      // Now the tree is not corrupt, though it may be unbalanced.
      // Rebalance
      rebalanceNode(chaseNode)
      //coalesceNodes(chaseNode, sibling, branchKey)
    }
    
  }
  
  
  //////////////////////////////////
  
  
  

  /** Returns the index of a child in a fork node.
    * 
    */
  // TDODO: Where else is this done, and is the method worth it?
  private def forkChildIndex(node: Node[A, B], child: Node[A, B])
      : Int =
  {
    var idx = 0
    while (idx <= node.num_keys && node.children(idx) != child)
    {
      idx += 1
    }
    idx
  }
  
  
  /** Creates a new root for two subtrees.
    *  
    * The method also joins the two nodes to the new root node.
    * 
    * @param left a node
    * @param k key between the two nodes
    * @param right a new node
    */
  private def insertIntoNewRoot(left: Node[A, B], k: A, right: Node[A, B])
      : Node[A, B] =
  {
    val root: Node[A, B] = mkRoot(left, k, right)
    //println(s"  insertIntoNewRoot left: $left right: $right")
    left.parent = Some(root)
    right.parent = Some(root)
    root
  }

  
  /** Inserts a new key and child into a fork.
    *  
    * The node must have space for the entries.
    * The method does not test for this.
    * 
    * @param node the node to insert elements into
    * @param index to insert at
    * @param key the key to be inserted
    * @param child the child to be inserted
    */
  private def forkAppend(
    node: Node[A, B],
    index: Int,
    key: A,
    child: Node[A, B]
  )
  {
    var i = node.num_keys
    //println(s"  forkAppend i: $i, left index: $left_index isLeaf: ${n.isLeaf}")

    // Skewed loop shunts keys right as expected
    // but the children are shunted from the advanced child
    // back to the child in front of the new key.
    while (i > index) {
      //println(s"loop: $i")
      ///println(s"node: $n")
      node.keys(i) = node.keys(i - 1)
      node.children(i + 1) = node.children(i)
      i -= 1
    }
    node.children(index + 1) = child
    node.keys(index) = key
    node.num_keys += 1
  }
  
  
  /** Inserts a new key and child into a fork node, then split.
    *
    * This assumes that it is known the node must be split. No test.
    * 
    * The method also handles insertion, and any subsequent splitting
    * in parents.
    * 
    * @param node the node to be split
    * @param insertIndex index in node data to insert the key child pair 
    * @param key the key to be inserted
    * @param child the node to be inserted as a child
    * @return the new root.
    */
  private def forkInsertAndSplit(
    root: Node[A, B],
    node: Node[A, B],
    insertIndex: Int,
    key: A,
    child: Node[A, B]
  )
      : Node[A, B] =
  {

    // Shunt the source node right to accept data.
    // On a fork this is not so simple, as the child to the right
    // need displacing.
    
    // Note: This could be done with a single speciality loop
    // Shunt the keys
    arrayShuntRight(node.keys, insertIndex, node.num_keys)
    // Shunt the children
    arrayShuntRight(node.children, insertIndex + 1, node.num_keys + 1)

    // fill the gap with insertion value
    node.keys(insertIndex) = key
    node.children(insertIndex + 1) = child
    
    //print(s"  forkInsertAndSplit temp filled: ")
    //node.keys.foreach{k => print (k + ", ")}
    //println()
    
    //print("node children:")
    //node.children.foreach{k => print (k + ",")}
    //println()
    
    // Create the new node
    var newNode: Node[A, B] = mkFork()
    
    // The node array now contains an oversized set of keys
    // and children.
    //
    // One more than is defined in BTree definitions. Where does this
    // get sliced?  The answer is, we must construct two arrays with a
    // trailing child. So one key is lost.
    //
    // Example: a fork node in a tree of branching factor of 4
    // (children) is full, so it has,
    //
    // 3 keys
    // 4 children
    //
    // Another child is added. In this algothithm, we use the same
    // arrays (temp arrays are would be another option for
    // implementing). So the new child, which overloads the node, has,
    //
    // 4 keys
    // 5 children (too many for the tree definition)
    //
    // If we split this, it can not be made into even size pieces. But
    // it can be made into two pieces which satisfy the definition of
    // branching factor 4 (minimum number of children = 2)
    //
    // Node 1
    // 1 key
    // 2 children
    //
    // Node 2
    // 2 keys
    // 3 children
    //
    // The children satisfy the BTree definition, and one key is lost.
    // The lost key is not damaging, it is moved up to the parent of
    // the two nodes.
    //
    // On a raw cut, the key would have been above the first node's
    // end child. It represents any data less than the child it is
    // above. When split it represents anything equal or greater than
    // the data in Node 1, so can work as the parent key to the node.
    //
    // Also, since the data for the new nodes came from the same node,
    // we know the child's data must be less than the data in the next
    // node.
    //
    // Once this is grasped, the problems are; how to perform this
    // awkward operation elegantly, and how to rearrange keys in a new
    // parent.
    
    // Reduce the old node by the cheap method of resetting the size
    // downwards (will need extra work in languages without garbage
    // collection)
    node.num_keys = forkMinKeys
    // K Prime is the key that must go to the parent
    // (this references the key after the one defined above)
    val k_prime = node.keys(forkMinKeys)

    //println(s"  forkInsertAndSplit node: $node kprime: $k_prime")


    // Starting at the child and key after the array elements
    // referenced above (so, index = forkMinChildren), copy data into
    // the new node.
    //
    // This limits using the forkMaxChildren (branching factor) to
    // cover remaining keys and most of the children. The last/extra
    // child will not be copied, and needs to be fixed after this.
    dualArrayCopy(
      node.keys,
      node.children,
      forkMinChildren,
      forkMaxChildren,
      newNode.keys,
      newNode.children,
      0
    )
    //println(s"nextEmptyIdx $nextEmptyIdx")
    // Copy the extra child across
    //
    // (in terms of children, forkMinChildren were copied ---
    // forkMinChildren is 1/2 branching factor.  therefore the index
    // of the empty end element is forkMinChildren)
    newNode.children(forkMinChildren) = node.children(forkMaxChildren)
    // println(s"forkMinChildren $forkMinChildren")

    // Set the new node key size.
    //
    // This, by construction, is node.num_keys + 1 (for the added
    // key child inserted)
    // - forkMinKeys - 1 (the removals for the new node) (in our
    // algorithm, which disallows odd-sized branching factors, it is
    // also forkMinKeys + 1 or, more tricky, forkMinChildren)
    newNode.num_keys = forkMinChildren
    //println(s"  forkInsertAndSplit newNode: $newNode")


    // set the new node parent
    newNode.parent = node.parent
    // set the parents of the children of the new node
    var i = 0
    while (i <= newNode.num_keys) {
      newNode.children(i).parent = Some(newNode)
      i += 1
    }

    //println(s"  forkInsertAndSplit: old: $node new: $newNode")

    // Insert a new key into the parent of the two nodes resulting
    // from the split, with the old node to the left and the new to
    // the right.
    insertIntoParent(root, node, k_prime, newNode)
  }

  
  /** Inserts a new node (leaf or internal node) into the B+ tree.
    * Returns the root of the tree after insertion.
    * 
    * @param root root node of the tree?
    * @param left a node, usually a node from which data has been split?
    * @param key the key between left and right (over the left node)
    * @param right a new node
    */
  private def insertIntoParent(
    root: Node[A, B],
    left: Node[A, B],
    key: A,
    right: Node[A, B]
  )
      : Node[A, B] =
  {

    val parentO = left.parent
    //println(s"  insertIntoParent... left:$left k:$k right:$right")

    // Catch nodes with parent of root
    if (parentO == None) insertIntoNewRoot(left, key, right)
    else {
      // Parent is not root
      val parent = parentO.get

      // Find the parent's pointer to the left
      // node.
      val left_index = forkChildIndex(parent, left)


      if (parent.num_keys < nodeMaxKeys) {
        // The new key fits into the node. Easy.
	forkAppend(parent, left_index, key, right)
        root
      }
      else {
	// insert, then split the node
	forkInsertAndSplit(root, parent, left_index, key, right)
      }
    }
  }

  
  /** Inserts a key value pair into a leaf node.
    * 
    * @return the altered leaf.
    */
  // Unused?
  // but should be?
  private def leafLowerIndexOf(leaf: Node[A, B], k: A)
      : Int =
  {
    // Find the insertion point
    var idx = 0
    while (idx < leaf.num_keys && ordering.lt(leaf.keys(idx), k)) {
      idx += 1
    }
    if(!ordering.lt(leaf.keys(idx), k)) idx
    else  -1
  }
  
  
  /** Returns the index of first occurrence of a key in this leaf.
    * 
    * @return the index of the first element of this leaf that is
    * equal (as determined by Ordering.equiv) to k, or -1, if none
    * exists.
    */
  private def leafIndexOf(leaf: Node[A, B], k: A)
      : Int =
  {
    // Find the insertion point
    var idx = 0
    while (idx < leaf.num_keys && !ordering.equiv(leaf.keys(idx), k)) {
      idx += 1
    }
    if( ordering.equiv(leaf.keys(idx), k)) idx
    else  -1
  }
  
  
  /** Optionally returns the value associated with a key.
    *  
    */
  private def leafGet(leaf: Node[A, B], k: A)
      : Option[B] =
  {
    // Find the insertion point
    var idx = 0
    while (idx < leaf.num_keys && ordering.equiv(leaf.keys(idx), k)) {
      idx += 1
    }
    if( ordering.equiv(leaf.keys(idx), k)) Some(leaf.vals(idx))
    else  None
  }
  
  
  /** Inserts a key value pair into a leaf node.
    * 
    * @return the altered leaf.
    */
  private def leafAppend(leaf: Node[A, B], k: A, v: B)
      : Node[A, B] =
  {
    // Find the insertion point
    var idx = 0
    while (idx < leaf.num_keys && ordering.lt(leaf.keys(idx), k)) {
      idx += 1
    }

    dualArrayShuntRight(leaf.keys, leaf.vals, idx, leaf.num_keys)
    
    // Insert the key and values
    // In leaves these have a 1:1 relationship
    leaf.keys(idx) = k
    leaf.vals(idx) = v
    leaf.num_keys += 1
    leaf
  }

  
  /** Inserts into a leaf and splits it.
    * 
    * Inserts a new key and pointer to a new record into a leaf so as
    * to exceed the tree's order, causing the leaf to be split in
    * half.
    * 
    * @param leaf the node to be split
    * @param k the key to be inserted
    * @param v the value to be inserted
    * @return 
    */
  // was leafAppendAfterSplitting
  private def leafAppendAndSplit(
    root: Node[A, B],
    leaf: Node[A, B],
    k: A,
    v: B
  )
      : Node[A, B] =
  {
    var newLeaf = mkLeaf()

    //println(s"  leafAppendAndSplit $leaf")
    // get the index of insertion
    var insertion_index = 0
    while(insertion_index < nodeMaxKeys && ordering.lt(leaf.keys(insertion_index), k)) {
      insertion_index += 1
    }
    //println(s"splitting... leaf: $leaf")

    //println(s"splitting... inserting at idx: $insertion_index")


    // Shunt some space for the insert.
    dualArrayShuntRight(leaf.keys, leaf.vals, insertion_index, leaf.num_keys)
    
    // Fill gap with insertion value
    leaf.keys(insertion_index) = k
    leaf.vals(insertion_index) = v

    //print("boosted keys:")
    // leaf.keys.foreach{k => print (k + ",")}
    // println()
    
    // Reduce the old node by the cheap method of resetting the size
    // downwards (will need extra work in languages without garbage
    // collection)
    leaf.num_keys = leafMinKeys
    // println(s"  leafAppendAndSplit oldleaf: $leaf")

    // Copy the tail data onto the new leaf
    // Have to go over the max keys as the array is filled - it's at
    // nodeMaxKeys + 1 which happens to be forkMaxChildren (the
    // branching factor).
    /*
     dualArrayCopy(
     leaf.vals,
     leaf.keys,
     leafMinKeys,
     forkMaxChildren,
     newLeaf.vals,
     newLeaf.keys,
     0
     )
     */
    //TODO: Test then remove the above
    dualArrayCopy(
      leaf.keys,
      leaf.vals,
      leafMinKeys,
      forkMaxChildren,
      newLeaf.keys,
      newLeaf.vals,
      0
    )
    // Pretty much System.arraycopy, it is claimed
    //Array.copy(leaf.keys, split, newLeaf.vals, 0, minKeys)
    //Array.copy(leaf.keys, split, newLeaf.keys, 0, minKeys)
    
    newLeaf.num_keys = leafMinKeys
    //println(s"  leafAppendAndSplit newleaf: $newLeaf")

    // Join the leaves together via nextLeaf
    newLeaf.nextLeaf = leaf.nextLeaf
    leaf.nextLeaf = Some(newLeaf)


    // Set the parents
    newLeaf.parent = leaf.parent
    val new_key = newLeaf.keys(0)

    insertIntoParent(root, leaf, new_key, newLeaf)
  }
  
  

  /** Finds the leaf possibly containing a key
    *  
    * And if key not found?
    * @return the leaf possibly containing the given key.
    */
  private def findLeaf(k: A)
      : Option[Node[A, B]] =
  {
    //findLeaf(treeRoot, k)
    var chaseNode = treeRoot

    while (!chaseNode.isLeaf) {
      var i = 0
      while(i < (chaseNode.num_keys) && ordering.gteq(k, chaseNode.keys(i))) {
        i += 1
      }
      chaseNode = chaseNode.children(i)
    }

    Some(chaseNode)
  }

  /** Selects the first leaf in this $coll.
    * 
    * Given the way this tree is implemented, this method will always
    * return something, even if the empty tree (return is an empty
    * leaf node, the sole node in the tree).
    *  
    * @return the first (left) leaf in this tree.
    */
  //TODO: Where else is this code used?
  private def firstLeaf()
      : Node[A, B] =
  {
    var chaseNode = treeRoot
    
    // Rise to leaves
    while (!chaseNode.isLeaf) {
      chaseNode = chaseNode.children(0)
    }
    chaseNode
  }
  
  
  /** Optionally returns the value associated with a key.
    *  
    */
  def get(key: A)
      : Option[B] =
  {
    // find(treeRoot.get, k, false)
    //find(treeRoot, k)
    val leafO : Option[Node[A, B]] = findLeaf(key)
    if (leafO == None) None
    else {
      val idx = leafO.get.keys.indexWhere(ordering.equiv(key, _))

      if (idx == -1) None
      else Some(leafO.get.vals(idx))
    }
  }
  
  /////////////////
  
  /** Test root for BTree compliance and adjust if necessary
    *  
    * This handles the situation if nodes are deleted and the
    * deletions cascade to root. If children drop below 2, this method
    * collapses the tree by a level.
    * 
    * (insertions reset a root, so do not need seperate consideration) 
    */
  private def rebalanceRoot() {
    // If the root is not empty, nothing to be done.
    // For example, deletions will have been done.
    // Only keyless roots concern us.

    // Moreover, if the root item is a leaf this is of no concern, as
    // our default is an empty leaf. In languages paying more
    // attention to NULL, (and no `empty object` conception) more work
    // may be needed here. See the main notes for the class.
    if (treeRoot.num_keys == 0 && !treeRoot.isLeaf) {

      // The definition of BTrees alows the root to have as few as 2 children.
      // If a tree is not a leaf at root (so contained >
      // branchingFactor elements) but has been reduced so the root
      // itself is empty of keys, there is a child imbalance. The root
      // contains one child, which will not function as a BTree.
      // There's an easy solution, though...
      
      // Promote
      // the first (only) child
      // as the new root.
      treeRoot.children(0).parent = None
      treeRoot = treeRoot.children(0)
    }
  }

  /** Coalesces a node with a sibling.
    *
    * The sibling node must be able to accept the additional entries
    * without exceeding the maximum. No protection.
    * 
    * This operation is needed when a node has become
    * too small after deletion.
    * 
    * @param src an underpopulated node to be emptied
    * @param dst a sibling node accepting the contents of the
    * underpopulated node
    * @param branchKey the parent key separating the two nodes
    */
  private def coalesceNodes(src: Node[A, B], dst: Node[A, B], branchKey: A) {

    if (src.isLeaf) {
      // leaf node

      // Append the keys and pointers of src to the dst.
      // straight copy, starting at dst.num_keys
      dualArrayCopy(
	src.keys,
	src.vals,
	0,
	src.num_keys,
	dst.keys,
	dst.vals,
	dst.num_keys
      )
      
      // Fix the pointer to nextLeaf
      dst.nextLeaf = src.nextLeaf
      
      // Fix the size
      dst.num_keys += src.num_keys
      
      //println(s"  coalesceNodes: leaf dst: ${toStringNode(dst)}")

      // Remove the src from the parent.
      val parent = src.parent.get
      forkRemove(parent, branchKey, src)
      
      // The parent is altered, so parent rebalance
      rebalanceNode(parent)
    }
    else {
      // fork node
      //println(s"  coalesceNodes: fork dst: ${toStringNode(dst)}")
      //println(s"  branchKey: $branchKey")
      val insertIdx = dst.num_keys

      // Append branchKey.
      dst.keys(insertIdx) = branchKey
      dst.num_keys += 1

      // Append pointers and keys from the neighbor.
      dualArrayCopy(
        src.keys,
        src.children,
        0,
        src.num_keys,
        dst.keys,
        dst.children,
        insertIdx + 1
      )
      
      // copy the extra child across (note: fork nodes only)
      dst.children(insertIdx + 1 + src.num_keys) = src.children(src.num_keys)
      
      // Set the new key count
      dst.num_keys += src.num_keys


      // All children must now point to the same parent.
      // dst.num_keys + 1 because forks have an extra child
      var i = 0
      val dstO = Some(dst)
      while (i < dst.num_keys + 1) {
        dst.children(i).parent = dstO
        i += 1
      }
      
      val parent = src.parent.get

      forkRemove(parent, branchKey, src)
      rebalanceNode(parent)
    }
  }



  /** Redistributes entries between two nodes.
    *  
    * The method moves a key/val pair from one node to a sibling node.
    * 
    * During deletion. a node can become too small. Usually, it would
    * be coalesced with another node, but sometimes the available
    * sibling node is too full to accept more entries. In which case,
    * a key/val pair is moved from a sibling into the node.
    *
    * Note that this method depends heavily on the `leftSiblingIdx`
    * being set to a state which reflects the `sibling` position. If
    * `leftSiblingIdx` is -1, the sibling is sited to the right, and
    * the first key is removed, not the last.
    * 
    * @param node the underpopulated node
    * @param sibling the node with too many entries
    * @param leftSiblingIdx the index of the sibling in the parent
    * @param branchKeyIndex the index in the parent of the key
    * separating the two nodes
    * @param branchKey the parent key separating the two nodes
    * @param k_prime the key between the dest node and it's neighbour
    */
  private def redistributeNodes(
    node: Node[A, B],
    sibling: Node[A, B],
    leftSiblingIdx: Int,
    branchKeyIndex: Int,
    branchKey: A
  )
  {
    //println(s"  redistributeNodes sibling $sibling branchKey $branchKey leftSiblingIdx $leftSiblingIdx")
    if (leftSiblingIdx != -1) {
      // Node has a sibling to the left.
      // Take the last (rightmost) key-pointer from the sibling
      // and prepend it into the node.
      
      if (!node.isLeaf) {
        //println(" redistributeNodes fork left")

	// In the node, shunt right the extra/last child
	node.children(node.num_keys + 1) = node.children(node.num_keys)
	// Shunt everything else to the right
	dualArrayShuntRight(
          node.keys,
          node.children,
          0,
          node.num_keys
        )
        
	// Move the last sibling child over
	node.children(0) = sibling.children(sibling.num_keys)
	// Fix the new child parent
	node.children(0).parent = Some(node)
	// Add the key from the parent (referring to that last child)
	node.keys(0) = branchKey
        
        // Update parent key which joins the nodes.
        // It now should be the value of the last key in the sibling.
        // (that last key now goes unused)
	node.parent.get.keys(branchKeyIndex) = sibling.keys(sibling.num_keys - 1)
        // Note: removing the sibling entry is done by resetting the key count
        // see below...
      }
      else {
        //println(s" redistributeNodes leaf left $node num: ${node.num_keys}")

	// Leaf. Easier...
        // Shunt everything to the right
        dualArrayShuntRight(
          node.keys,
          node.vals,
          0,
          node.num_keys
        )
        //println(s" redistributeNodes leaf left finished node : $node")

	// Move the last key/val over
	node.vals(0) = sibling.vals(sibling.num_keys - 1)
	node.keys(0) = sibling.keys(sibling.num_keys - 1)

        // Update parent key which joins the nodes.
        // It can be a duplicate of the key which was moved
        node.parent.get.keys(branchKeyIndex) = node.keys(0)
        // Note: removing the sibling entry is done by resetting the key count
        // see below...
      }
    }
    else {
      // Node is the leftmost child. Siblings are to the right.
      // Take the first (leftmost) key-pointer pair from the sibling
      // and append it to the node.
      
      
      if (node.isLeaf) {
        //println(" redistributeNodes leaf right")
        // take the key/val and append to the node end
	node.keys(node.num_keys) = sibling.keys(0)
	node.vals(node.num_keys) = sibling.vals(0)
        
        // Update the parent key which joins the nodes.
        // It is now a duplicate of the key at the start of the sibling
	node.parent.get.keys(branchKeyIndex) = sibling.keys(1)
        
        // remove the entry from the sibling, by shunting left
        dualarrayShuntLeftUntil(
          sibling.keys,
          sibling.vals,
          0,
          sibling.num_keys
        )
      }
      else {
        // fork node
        //println(" redistributeNodes fork right")

        // end key is the old key from the parent, which split the nodes
	node.keys(node.num_keys) = branchKey
        // take the end child
        //node.num_keys + 1, for the extra child in forks.
	node.children(node.num_keys + 1) = sibling.children(0)
        // set the parent of the new child of the node.
	node.children(node.num_keys + 1).parent = Some(node)
        
        // Update parent key which joins the nodes.
        // It now should be the value of the first key in the sibling.
        node.parent.get.keys(branchKeyIndex) = sibling.keys(0)
        
        // remove the entry from the sibling, by shunting left
        dualarrayShuntLeftUntil(
          sibling.keys,
          sibling.children,
          0,
          sibling.num_keys
        )
        // Shunt the extra child (only needed in fork nodes)
        sibling.children(sibling.num_keys - 1) = sibling.children(sibling.num_keys)
      }
    }

    // set the key count for both nodes
    node.num_keys +=1
    // Note: in languages without garbage collection the
    // lost sibling entry may need to be free'd
    sibling.num_keys -= 1
  }


  /// Delete //
  
  
  /** Returns the index of the sibling to the left of a node.
    * 
    * It is possible for no left sibling to exist, if the given node
    * is the leftmost (first) child.
    * 
    * This method is mainly of use in deletion. 
    * 
    * @param node the node to be searched
    * @param child the target child 
    * @return the index of the sibling to the left of the child
    * in the parent, or -1 if no such sibling exists. 
    */
  private def leftSiblingIndex(parent: Node[A, B], child: Node[A, B])
      : Int =
  {
    //println(s"  leftSiblingIndex ${parent.num_keys}")
    var i = 0
    while((i <= parent.num_keys) && parent.children(i) != child) {
      i += 1
    }
    // Protection code. Could be removed in assured working implementations.
    if (parent.children(i) == child) i - 1
    else {
      throw new Exception(s"Search for nonexistent pointer to node in parent.")
    }

  }


  /** Removes a key from a node.
    *  
    * @return index of removal
    */
  private def nodeRemoveKey(node: Node[A, B], k: A)
      : Int =
  {
    //println(s"nodeRemoveKey $node, $k keysize: ${node.num_keys}")

    // Find the key
    var i = 0
    while (!ordering.equiv(node.keys(i), k)) {i += 1}
    //println(s"nodeRemoveKey idx: $i")
    
    // Shunt the other keys back over
    arrayShuntLeftUntil(node.keys, i, node.num_keys)

    i
  }
  
  
  /** Removes a key from a leaf.
    * 
    * The key must be in the leaf, no protection.
    * @param k the key to find 
    */
  private def leafRemove(node: Node[A, B], key: A) {
    //println(s"leafRemove $node k: $k")
    var i = nodeRemoveKey(node, key)

    arrayShuntLeftUntil(node.vals, i, node.num_keys)

    // One key fewer.
    node.num_keys -= 1
    size -= 1
  }
  
  
  /** Remove a key and child from a fork.
    *  
    * A basic method, which assumes the key and child have been
    * previously identified.
    * 
    * The key and child must be in the node, no protection.
    *
    * @param the node 
    * @param k the key to be removed
    * @param child the child to be removed
    */
  private def forkRemove(node: Node[A, B], k: A, child: Node[A, B]) {
    //println(s"forkRemove k: $k")

    nodeRemoveKey(node, k)

    var i = 0
    //TODO: Reference equivalence?
    while (node.children(i) != child) {i += 1}
    
    // Remove the pointer and shift other pointers accordingly.
    // (One more child, for fork node)
    arrayShuntLeftUntil(node.children, i, node.num_keys + 1)

    // One key fewer.
    node.num_keys -= 1
  }

  ////////////////////////////////////////
  

  
  /** Rebalances nodes after entries have been deleted.
    * 
    * Should be called on nodes after every deletion.
    * 
    * @param node can be a leaf or a fork.
    * @param k the key which was deleted.
    */
  private def rebalanceNode(node: Node[A, B])
  {
    if (node == treeRoot) {
      // the root node. A special case.
      rebalanceRoot()
    }
    else {
      // Deletion from any node besides root.


      // Find the capacity of the node
      val minKeys = if (node.isLeaf) leafMinKeys else forkMinKeys
      
      if(node.num_keys < minKeys) {
      	// Node is under populated.

	// Get the parent
	var parentO = node.parent
        // Protection code. Can be removed if setting of parents is
        // assured correct.
	val parent =
	  if (parentO == None) {
	    throw new Exception("Error, searching for sibling but no parent")
	  }
	  else {
	    parentO.get
	  }

        // find the neighbour to the left
	val leftSiblingIdx = leftSiblingIndex(parent, node)
        // Set the index for the control key. This is the key above
        // the left sibling.  Therefore, any key in the sibling or
        // below will be below this key, and any key in the given node
        // will be above.
        //
        // However, there is a trick here. If the node is leftmost
        // (leftSiblingIdx == -1) the index is set to the key above
        // the node. This is because the node will be balanced with
        // the node to the right (so 'node' is the 'left sibling')
	val branchKeyIndex = if(leftSiblingIdx == -1) 0 else leftSiblingIdx
	val branchKey: A = parent.keys(branchKeyIndex)
        // Get the sibling. Usually a left sibling but, if we found no left
        // sibling, it's the right sibling
	val sibling =
          if(leftSiblingIdx == -1) parent.children(1)
          else parent.children(leftSiblingIdx)

	val maxKeys = if(node.isLeaf) branchingFactor else branchingFactor - 1

        // println(s"  rebalanceNode node.num_keys: ${node.num_keys} sibling.num_keys ${sibling.num_keys} leftSiblingIdx $leftSiblingIdx")
        //println(s"  node: ${toStringNode(node)}")
        //println(s"  sibling: ${toStringNode(sibling)}")
        //println(s"  pre coalesce:\n${toFrameString()}")

        //TODO: Surely redistribution should be favoured over
        // coaleseing? It is potentially lower weight,
        // and coalescing guarantees a recursive upwards delete,
        // which redistribution does not? Mind you,
        // redistribution needs a complete array shunt.
        // Information, please?
        // To do that, check if sibling node has keys to spare...
	if ((sibling.num_keys + node.num_keys) < maxKeys) {
	  // Coalesce
          //println(s"  coalesce... $leftSiblingIdx")
          if (leftSiblingIdx == -1) {
            //println("  coalesceNodes: Nodes reversed!")
            // If the underpopulated node is leftmost, swap nodes so
            // the destination is the node itself, and the right
            // sibling will be merged into it.
            coalesceNodes(sibling, node, branchKey)

          }
          else {
            // Otherwise, merge the node contents into the left sibling node
            coalesceNodes(node, sibling, branchKey)
            //println(s"  post coalesce:\n${toFrameString()}")

          }
	}
	else {
	  // Redistribute
          //println("  redist...")
	  redistributeNodes(node, sibling, leftSiblingIdx, branchKeyIndex, branchKey)
	}
      }
    }
  }
  
  
  
  /** Removes a key from this $coll, returning the value associated
    * previously with that key as an option.
    *
    */
  def remove(k: A)
      : Option[B] =
  {
    val leafO: Option[Node[A, B]] = findLeaf(k)
    
    if (leafO == None) None
    else {
      
      val leaf = leafO.get
      val valO = leafGet(leaf, k)
      
      if (valO == None) None
      else {
        leafRemove(leaf, k)
        rebalanceNode(leaf)//, k)
        valO
      }
    }
  }
  
  
  /** Removes a key from this $coll.
    *  
    * If the key does not exist, does nothing
    */
  def -= (k: A)
      : DemonstrationBPlusTree[A, B] =
  {
    val leafO: Option[Node[A, B]] = findLeaf(k)
    if (leafO != None) {
      val leaf = leafO.get
      val idx = leafIndexOf(leaf, k)
      if(idx != -1) {
        leafRemove(leaf, k)
        rebalanceNode(leaf)//, k)
      }
    }
    this
  }
  
  
  /** Removes all elements produced by an iterator from this $coll.
    */
  def --= (xs: TraversableOnce[A])
  {
    xs.foreach(-=)
  }
  

  /** Adds an element to this $coll.
    *   
    * If the key already exists, the attempt to append is ignored.
    *   
    * @param k the key to add
    * @param v the value to add
    */
  def += (k: A, v: B)
      : DemonstrationBPlusTree[A, B] =
  {
    if (get(k) != None) this
    else {
      val leafO = findLeaf(k)
      var leaf = leafO.get
      if (leaf.num_keys < nodeMaxKeys) {
        // the leaf has space for the new key value
	leaf = leafAppend(leaf, k, v)
      }
      else {

	// no space, so the leaf must be split.
	treeRoot = leafAppendAndSplit(treeRoot, leaf, k, v)
      }

      // NB: While it is inelegant and not robust to
      // set a size varable at a class access point,
      // the BTree uses two separate algorithms for
      // appending.
      size += 1

      this
    }
  }

  /** Adds an element to this $coll.
    *   
    * If the key already exists, the attempt to append is ignored.
    *   
    * @param kv a tuple of (key, value) to add
    */
  // A Scala speciality really, if you don't get it,
  // ignore for now. Helps with testing.
  def += (kv: (A, B))
      : DemonstrationBPlusTree[A, B] =
  {
    += (kv._1, kv._2)
  }

  /** Removes all bindings from this tree.
    *  
    * After this operation has completed, the tree will be empty. 
    */
  def clear() {
    treeRoot = mkLeaf()
    //Node.leaf(branchingFactor) //mkLeaf()
    size = 0
  }
  
  
  /** Selects an interval of elements.
    *
    * The method doesn't guarentee to match the key values. It will
    * start and stop when they are exceeded i.e.
    * 
    * {{{from < indexOf(x) < until}}}
    * 
    * @return an array containing the elements greater than or equal
    * to index `from` extending up to (but not including) index
    * `until` of this $coll. 
    */
  def slice(from: A, until: A)
      : Array[B] =
  {
    // Get the leaf, go to it
    val fromLeafO = findLeaf(from)
    if (fromLeafO == None) Array.empty[B]
    else {
      var fromLeaf = fromLeafO.get
      
      // Find start position
      var i = 0
      while (i < fromLeaf.num_keys && ordering.lt(fromLeaf.keys(i), from)) {
	i += 1
      }
      if (i == fromLeaf.num_keys) Array.empty[B]
      else {
        
        // Ok, we have a start index (i).
        var cont = true
	var b = new scala.collection.mutable.ArrayBuffer[B]()
        
	do {
	  while(i < fromLeaf.num_keys && ordering.lt(fromLeaf.keys(i), until)) {
	    b += fromLeaf.vals(i)
	    i += 1
	  }
	  val leafO = fromLeaf.nextLeaf
          //println(s"cont: $leafO")
          // If we didn't find a new leaf, or exceeded key values, break out
	  if (leafO == None || ordering.gteq(fromLeaf.keys(i), until)) cont = false
	  else {
	    fromLeaf = leafO.get
	    i = 0
	  }
	} while (cont == true)
	  b.toArray
      }
    }
  }
  
  
  /** Updates the value in a key/value pair to this $coll.
    *  
    * If the $coll has no mapping for the key, the update returns
    * silently.
    *  
    */
  def update(k: A, v: B)
  {
    // Get the leaf, go to it
    val leafO = findLeaf(k)
    if (leafO != None)
    {
      val leaf = leafO.get
      val idx = leafIndexOf(leaf, k)
      if (idx != -1) {
        leaf.vals(idx) = v
      }
    }
  }

  //////////////////////////////////
  
  // Iterators //
  // The iterators are mainly of interest to Scala implementations
  // where they form an important base part of the collection API.
  // However, C++/Java implementations will have an interest, and
  // maybe others should too.
  //
  // Note: the foreach method can be implemented in terms of iterators,
  // although here it is not.
  
  /** Returns an Iterator over the elements in this $coll.
    *  
    */
  def iterator
      : Iterator[(A, B)] =
    new Iterator[(A, B)] {
      var currentLeaf = firstLeaf()
      var currentPos = 0
      def hasNext()
          : Boolean =
      {
        if (currentPos < currentLeaf.num_keys) true
        else {
          val nextLeafO = currentLeaf.nextLeaf
          if (nextLeafO == None) false
          else {
            currentLeaf = nextLeafO.get
            currentPos = 0
            currentPos < currentLeaf.num_keys
          }
        }
      }
      
      def next()
          : (A, B) =
      {
        if (hasNext) {
          val res = (currentLeaf.keys(currentPos), currentLeaf.vals(currentPos))
          currentPos += 1
          res
        }
        else Iterator.empty.next()
      }
    }
  
  // There is perhaps some minor gain to be made
  // having dedicated keys and map iterators.
  
  /** Creates an iterator for all keys.
    *
    * @return an iterator over all keys. 
    */
  def keysIterator
      : Iterator[A] =
    new Iterator[A] {
      var currentLeaf = firstLeaf()
      var currentPos = 0
      def hasNext()
          : Boolean =
      {
        if (currentPos < currentLeaf.num_keys) true
        else {
          val nextLeafO = currentLeaf.nextLeaf
          if (nextLeafO == None) false
          else {
            currentLeaf = nextLeafO.get
            currentPos = 0
            currentPos < currentLeaf.num_keys
          }
        }
      }
      
      def next()
          : A =
      {
        if (hasNext) {
          val res = currentLeaf.keys(currentPos)
          currentPos += 1
          res
        }
        else Iterator.empty.next()
      }
    }
  
  /** Creates an iterator for all vals.
    *
    * @return an iterator over all vals. 
    */
  def valsIterator
      : Iterator[B] =
    new Iterator[B] {
      var currentLeaf = firstLeaf()
      var currentPos = 0
      def hasNext()
          : Boolean =
      {
        if (currentPos < currentLeaf.num_keys) true
        else {
          val nextLeafO = currentLeaf.nextLeaf
          if (nextLeafO == None) false
          else {
            currentLeaf = nextLeafO.get
            currentPos = 0
            currentPos < currentLeaf.num_keys
          }
        }
      }
      
      def next()
          : B =
      {
        if (hasNext) {
          val res = currentLeaf.vals(currentPos)
          currentPos += 1
          res
        }
        else Iterator.empty.next()
      }
    }
  
  ///////////////////////////////////////////////////////
  
  /** Applies a function f to every element in the tree.
    * 
    */
  def foreach(f: (A, B) ⇒ Unit)
      : Unit =
  {
    var chaseNode = treeRoot
    
    // Rise to leaves
    while (!chaseNode.isLeaf) {
      chaseNode = chaseNode.children(0)
    }
    
    var leafO: Option[Node[A, B]] = Some(chaseNode)
    var i = 0
    while (leafO != None) {
      
      while(i < chaseNode.num_keys) {
        chaseNode = leafO.get
        f(chaseNode.keys(i), chaseNode.vals(i))
        i += 1
      }
      leafO = chaseNode.nextLeaf
    }
  }
  
  
  /** Applies a function f to every value inserted into the tree.
    * 
    */
  def foreachVal(f: (B) ⇒ Unit)
      : Unit =
  {
    var chaseNode = treeRoot
    
    // Rise to leaves
    while (!chaseNode.isLeaf) {
      chaseNode = chaseNode.children(0)
    }
    
    var leafO: Option[Node[A, B]] = Some(chaseNode)
    var i = 0
    while (leafO != None) {
      chaseNode = leafO.get
      while(i < chaseNode.num_keys) {
        f(chaseNode.vals(i))
        i += 1
      }
      leafO = chaseNode.nextLeaf
      //println(s"cont: $leafO")
    }
  }
  
  
  /** Runs function f on every node in the tree.
    *  
    * Note: this displays the internal structure of the
    * tree. More for debugging than external use.
    */
  private def foreachNode(node: Node[A, B], f: (Node[A, B]) ⇒ Unit)
      : Unit =
  {
    f(node)
    if (!node.isLeaf) {
      node.children.foreach{child => foreachNode(child, f)}
    }
  }

  
  /** Runs function f on every node in the tree.
    *  
    * Note: this displays the internal structure of the
    * tree. More for debugging than external use.
    */
  def foreachNode(f: (Node[A, B]) ⇒ Unit): Unit  =  foreachNode(treeRoot, f)

  
  /** Runs function f on every node in the tree, with depth parameter.
    *  
    * Note: this displays the internal structure of the
    * tree. More for debugging than external use.
    */
  private def foreachNodeAndDepth(node: Node[A, B], f: (Node[A, B], Int) ⇒ Unit, depth: Int)
      : Unit =
  {
    f(node, depth)
    if (!node.isLeaf) {
      for(i <- 0 to node.num_keys) {
        foreachNodeAndDepth(node.children(i), f, depth + 1)
      }
    }
  }
  
  
  /** Runs function f on every node in the tree, with depth parameter.
    *  
    * Note: this displays the internal structure of the
    * tree. More for debugging than external use.
    */
  def foreachNodeAndDepth(f: (Node[A, B], Int) ⇒ Unit, depth: Int)
      : Unit = foreachNodeAndDepth(treeRoot, f, depth)

  
  /** Adds a string representation of node data to a string builder.
    * 
    */
  private def mkStringNode(b: StringBuilder, node: Node[A, B])
      : StringBuilder =
  {
    if (node.isLeaf) {
      var first = true
      b ++= "Leaf("
      for(i <- 0 until node.num_keys) {
        if (!first) b append ", "
        else first = false
        b append node.keys(i)
        b append " -> "
        b append node.vals(i)
      }
      b += ')'
    }
    else {
      var first = true

      b ++= "Fork("
      for(i <- 0 until node.num_keys) {
        if (!first) b append ", "
        else first = false
        b append node.keys(i)
      }
      b += ')'
    }
    b
  }

  /** Returns a representation of a node as a string.
    * 
    */
  def toStringNode(node: Node[A, B])
      : String =
  {
    mkStringNode(new StringBuilder, node).result()
  }
  
  /** Adds a string representation of the $coll elements to a string builder.
    * 
    */
  private def mkStringLeaf(b: StringBuilder, node: Node[A, B])
      : StringBuilder =
  {
    for(i <- 0 until node.num_keys) {
      b append ", "
      b append node.keys(i)
      b append " -> "
      b append node.vals(i)
    }
    b
  }
  
  
  /** Adds data on the $coll structure to a string builder.
    * 
    */
  def mkFrameString(b: StringBuilder, node: Node[A, B])
      : StringBuilder =
  {
    def printNode(n: Node[A, B], depth: Int)
    {
      b ++= "  " * depth
      //n.mkString(b)
      mkStringNode(b, n)
      b ++= "\n"
    }
    foreachNodeAndDepth(node, printNode _, 0)
    b
  }
  
  
  /** Returns a representation of the $coll structure as a string.
    * 
    */
  def toFrameString()
      : String =
  {
    //    mkFrameString(new StringBuilder(), treeRoot.get).result()
    mkFrameString(new StringBuilder(), treeRoot).result()
  }
  
  
  /** Adds data on all elements to a string bulder
    * 
    */
  def mkString(b: StringBuilder)
      : StringBuilder =
  {
    var chasingNode = treeRoot
    // Track from the supplied node to the leaves.
    while (!chasingNode.isLeaf) {chasingNode = chasingNode.children(0)}
    

    mkStringLeaf(b, chasingNode)
    while(chasingNode.nextLeaf != None) {
      chasingNode = chasingNode.nextLeaf.get
      mkStringLeaf(b, chasingNode)
    }
    // drop the surplus comma
    b.drop(2)
  }
  
  
  /** Returns a representation of all $coll elements as a string.
    * 
    */
  override def toString()
      : String =
  {
    val b = mkString(new StringBuilder())
    "DemonstrationBTree(" + b.result() + ")"
  }
  
}//DemonstrationBPlusTree



/*
 object OrderingInt extends Ordering[Int] {
 def compare(a:Int, b:Int) = b - a
 }
 */
object DemonstrationBPlusTree {

  // If this tree is to be converted for specific types
  // in Scala, fill out these, then convert the node
  // building  methods, as described in the mkLeaf method.
  //val emptyKey : A =
  //val emptyValue : B =

  /** A collection of type DemonstrationBPlusTree.
    *
    * @param minChildren Minimum number of children in an internal
    * (fork) node. The max ("branchingFactor/order") is set to
    * 2*minChildren
    */
  def apply[A, B](minChildren: Int)(implicit ordering: Ordering[A], m1: ClassTag[A], m2: ClassTag[B])
      : DemonstrationBPlusTree[A, B] =
  {
    new DemonstrationBPlusTree(minChildren)(m1, m2, ordering)
  }

  /**  A collection of type DemonstrationBPlusTree.
    * 
    * This factory constructor uses a bulk-loading method to initially
    * populate a tree. This is potentially fast for large bulk loads
    * (though the current Scala implementation may not be so)
    * 
    *
    * @param minChildren Minimum number of children in an internal
    * (fork) node. The max ("branchingFactor/order") is set to 2*minChildren
    * @param density number of given items per leaf, for the inital data
    * loading. Must be leafMinKeys => density <= nodeMaxKeys
    */
  def bulkload[A, B](minChildren: Int, density: Int, xs: TraversableOnce[(A, B)])(implicit ordering: Ordering[A], m1: ClassTag[A], m2: ClassTag[B])
      : DemonstrationBPlusTree[A, B] =
  {
    val t = new DemonstrationBPlusTree(2)(m1, m2, ordering)
    t.bulkLoad(density, xs)
    t
  }

  
  import tree.StopWatch
  
  def main(args: Array[String])
  {

    /** Data with keys in reverse sequence.
      * 
      * BTrees construct in a very different way with reverse keys, so
      * this is good stress test.
      */
    val reverseData = Seq(
      (22, "Wind Chimes"),
      (21, "Fun, Fun, Fun"),
      (20, "Forever"),
      (19, "Wipeout"),
      (18, "All Summer Long"),
      (17, "Sail On, Sailor"),
      (16, "Cabin Essence"),
      (15, "Cottonfields"),
      (14, "Our Prayer"),
      (13, "Surfs Up"),
      (12, "Surfing Safari"),
      (11, "Don't worry baby"),
      (10, "The Warmth of the Sun"),
      (9, "Barbara Ann"),
      (8, "California Girls"),
      (7, "Sloop John B"),
      (6, "Vegetables"),
      (5, "Little deuce coupe"),
      (4, "Wouldn't it be nice"),
      (3, "Heros and Villains"),
      (2, "I get around"),
      (1, "Good vibrations")
    )

    // Test trees are mainly built with minChildren = 2
    // (so branchingFactor = 4) to provoke early node splitting
    val t = DemonstrationBPlusTree[Int, String](2)

    println("    --------------------------")
    println("      DemonstrationBPlusTree")
    println("    --------------------------")
    println("(this demo works with data in reverse order, for a little more realism)")
    println

    // toFrameString()
    println("print a tree loaded with data, using toFrameString():")
    reverseData.foreach(t += _)
    println(t.toFrameString())
    println

    // get()
    println("get() can select elements by key:")
    println("  (should get (7, 'Sloop John B'))")
    println(s"  ${t.get(7)}")
    println

    // iterator
    println("an iterator can walk elements:")
    println("  (should print 1-22 elements)")
    // test could make a Seq and test the size
    val it = t.iterator
    while(it.hasNext){
      val e : Tuple2[Int, String] = it.next
      println("  " + e._1 + " -> " + e._2 + ", ")
    }
    println

    // slice()
    println("slice can select a range of elements by key:")
    println("  (should slice 3-7)")
    t.slice(3, 7).foreach{x: String => print(x + ", ")}
    println
    println


    println("timing test for iterative vs. bulkloading:")
    println("  (loading only a few elements, bulkloading will likely loose)")

    StopWatch.test("Tree build using  '+='") {
      val t = DemonstrationBPlusTree[Int, String](2)
      reverseData.foreach(t += _)
    }//stopwatch
    
    
    StopWatch.test("Tree build using  bulkload") {
      val t = DemonstrationBPlusTree.bulkload(2, 2, reverseData)
    }//stopwatch
    println
    
  }//main

}//DemonstrationBPlusTree
