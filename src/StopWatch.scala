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


/** Time a stretch of code.
 * 
 * {{{
 * StopWatch.start("test1")
 * StopWatch.times(6) {
 * ... sometest
 * }
 * StopWatch.stop()
 * }}}
 * 
 * or
 * 
 * {{{
 * StopWatch.test("test1", 6) {
 * ... sometest
 * }
 * }}}
 * 
 * Little and easy for little, easy people.
 */
//TODO: The Apache.commons version?
object StopWatch {
  
      var s = 0L
      var id : String = ""
        
  def start(id: String = "") {
    StopWatch.this.id = id
   s = System.nanoTime();
  }    
      
      def times(multiply : Int)(block : => Unit) = {
         for (i <- 0 until multiply) {block}
      }
      
      def stop(formatTime: Boolean = true) {
        val diff = System.nanoTime() - s
      val tStr = if (formatTime)  humanFormat(diff)
                 else diff.toString + " ns"
      println(id + ": time taken :" + tStr)
      }
      
     def test(id: String = "", multiply : Int = 1)(block : => Unit) { 
       start(id)
      times(multiply)(block) 
      stop()
     }
     
     //private
     def humanFormat(time: Long) : String = {
       time match {
                  case x if (x < 1000L) => s"${x} nanosecs"
                  case x if (x < 1000000L) => f"${x/1000D}%g microsecs"
                  case x if (x < 1000000000L) => f"${x/1000000D}%g millisecs"
                  case x if (x < 60000000000L) => f"${x/1000000000D}%g secs"
                  case x if (x < 1000000000000L) => f"${x/60000000000D}%g minutes"
                  case x => s"${x/1000000000000L} seconds"
       }
       
     //String.format("%d:%02d:%02d", s/3600, (s%3600)/60, (s%60))
     }
     
}//Timer
