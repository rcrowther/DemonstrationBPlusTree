package tree

trait Fixture {

  /** Test trees are built with minChildren = 2
    * (so branchingFactor = 4) to provoke early node splitting
    */
  def bPlusTree = DemonstrationBPlusTree[Int, String](2)


  /** Data with keys in forward sequence.
    * 
    * This set deliberately builds with a lone child on an end fork.
    */
  val data = Seq(
    (1, "Good vibrations"),
    (2, "I get around"),
    (3, "Heros and Villains"),
    (4, "Wouldn't it be nice"),
    (5, "Little deuce coupe"),
    (6, "Vegetables"),
    (7, "Sloop John B"),
    (8, "California Girls"),
    (9, "Barbara Ann"),
    (10, "The Warmth of the Sun"),
    (11, "Don't worry baby"),
    (12, "Surfing Safari"),
    (13, "Surfs Up"),
    (14, "Our Prayer"),
    (15, "Cottonfields"),
    (16, "Cabin Essence"),
    (17, "Sail On, Sailor"),
    (18, "All Summer Long"),
    (19, "Wipeout"),
    (20, "Forever"),
    (21, "Fun, Fun, Fun"),
    (22, "Wind Chimes"),
    (23, "Let Him Run Wild"),
    (24, "Be True to Your School"),
    (25, "Girls on the Beach"),
    (26, "Caroline, No")
  )

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

}//Fixture
