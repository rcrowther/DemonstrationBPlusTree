==========================
The DemonstrationBPlusTree
==========================

Intro
=====

Will probably run on any post-Java7 and modern Scala. There's nothing special, and no dependencies, unless testing (put a Scalatest jar in the `/lib` folder).

Comes with Create/Read/Update/Delete methods, and initial bulk-loading. Not just some graduate assignment or other coding exercise, but intended for study.

Notes
=====
The B-Tree is deliberately coded in a clumsy way. It uses Arrays, which have little place in Scala. None of the methods use explicit recursion. Typing is explicit, but again, deliberately crude. No effort is expended on improving efficiency. These decisions were made because the code is intended as a clear demonstration of how a BPlusTree works, not as sturdy, extensible base code.

However, the code should be easy to translate into other languages, and is massively annotated. Yeah, I wrote it, but for education and demonstration I'd figure it beats anything on the web. And I've looked round a lot (books are another matter, but I cann't afford books).


Additional material
===================
If a library containing Scalatest is added, the code includes some tests. These are rather crude, but a start. One interesting test contains code to provoke specific changes in a BPlusTree, including splits and re-distributions. It could serve as a start for other test code.

If the code is compiled as Scala, then the DemonstrationBPlusTree class contains a main() method running a short demo, so, ::

run tree.DemonstrationBPlusTree

...or however your IDE/build tool runs classes. This, or similar, will work, ::

scala -toolcp <path to the compile> tree.DemonstrationBPlusTree




References
==========

Wikipedia
---------
`Wikipedia BTrees`_
 
`Wikipedia BPlusTree`_


An alternative
--------------
A Scala immutable BTree. Unfortunately, heavily typed Scala can can confuse code outlines, and has here (the implementation uses generic arrays, if that means anything to you). A masterwork, likely for non-Scala users obscure,

`Scala Immutable BTree`_


.. _Wikipedia BTrees: https://en.wikipedia.org/wiki/B-tree
.. _Wikipedia BPlusTree: https://en.wikipedia.org/wiki/B%2B_tree 
.. _Scala Immutable BTree: https://github.com/zilverline/scala-btree
