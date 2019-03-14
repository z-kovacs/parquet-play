import java.time.Duration
import java.time.temporal.ChronoUnit

import scala.io.StdIn

object DSLExample {

  def mid(ccy: String): SignalExpr[Double] = Mid(ccy)

  def bid(ccy: String): SignalExpr[Double] = Bid(ccy)

  def avg(signalExpr: SignalExpr[Double]) = Avg(signalExpr)

  //////////////

  def fsmWOState() = new EmptyFSM2[AnyRef](null)

  def fsmWithState[State](s: State) = new EmptyFSM2[State](s)

  //implicit def producing[S]( t:(S,List[SignalAssignment[_]]) ) = t match { case(s,newSignals) => Product(s,newSignals) }
  //implicit def producing[S]( l:List[SignalAssignment[_]] ) = Product[AnyRef](null,l)

}


sealed trait SignalExpr[T] {
  def <--(v: T) = SignalAssignment[T](this, v)
}

case class Mid(ccy: String) extends SignalExpr[Double]

case class Bid(ccy: String) extends SignalExpr[Double]

case class Avg(signal: SignalExpr[Double]) extends SignalExpr[Double]

case class SignalAssignment[T](signalExpr: SignalExpr[T], value: T) {


}

case class Args1[S, T](s: S, signalExpr: SignalExpr[T]) {
  def runs(fun: (S, T) => List[SignalAssignment[_]]) = FSMWithInPlaceState[S](s, List(signalExpr), (s: S, l: List[_]) => fun(s, l(0).asInstanceOf[T]))
}

case class Args2[S, T, Y](s: S, signalExpr: SignalExpr[T], signalExpr2: SignalExpr[Y]) {
  def runs(fun: (S, T, Y) => List[SignalAssignment[_]]) = FSMWithInPlaceState[S](s, List(signalExpr, signalExpr2), (s: S, l: List[_]) => fun(s, l(0).asInstanceOf[T], l(1).asInstanceOf[Y]))
}


case class EmptyFSM2[S](s: S) {
  def using[T](signalExpr: SignalExpr[T]): Args1[S, T] = Args1(s, signalExpr)

  def using[T, Y](signalExpr: SignalExpr[T], signalExpr2: SignalExpr[Y]): Args2[S, T, Y] = Args2(s, signalExpr, signalExpr2)
}

//pro: in advance we knoew deps
//pro: typesafe construcitons
//pro: in code we can do any maths magic
//con: generated signals are not known pre-run => we cannot statically build an execution tree
case class FSMWithInPlaceState[T](s: T, list: List[SignalExpr[_]], func: (T, List[_]) => List[SignalAssignment[_]]) {

  val state: T = s

  def resolve(signalExpr: SignalExpr[_]): Any =
    signalExpr match {
      case Mid(ccy) => 0.1
      case Bid(ccy) => 0.2
    }

  def dependecies = list

  def execute(): List[SignalAssignment[_]] = {
    //resolve signals, ad then call

    val resolved = list.map(resolve _)

    func(state, resolved)
  }
}

/*
//to compile som code and run programmatically
object CompileAndRun{

  import scala.tools.nsc._

  import java.io._

  val g = new Global(new Settings())

  val run = new g.Run

  run.compile(List("test.scala"))  // invoke compiler. it creates Test.class.

  val classLoader = new java.net.URLClassLoader(
    Array(new File(".").toURI.toURL),  // Using current directory.
    this.getClass.getClassLoader)

  val clazz = classLoader.loadClass("Test") // load class

  clazz.newInstance  // create an instance, which will print Hello World.
}
*/

object Test {

  import DSLExample._

  def formatDuration(duration: Duration): String = {
    val seconds = duration.getSeconds
    val absSeconds = Math.abs(seconds)
    val positive = "%d:%02d:%02d %02d ns".format(absSeconds / 3600, (absSeconds % 3600) / 60, absSeconds % 60, duration.getNano)
    if (seconds < 0) "-" + positive
    else positive
  }

  def time[R](block: => R): (Long, R) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    val d = Duration.of(t1 - t0, ChronoUnit.NANOS)
    println("Elapsed time: " + formatDuration(d))
    (t1 - t0, result)
  }

  def main(args: Array[String]): Unit = {


    //TODO nostate fsm is separate usecase, so we don't have to ignore the state arg, and possibly we cannot return with
    // an object, and the implicits are nicer too
    //no state
    val fsm1 = fsmWOState using (mid("USD")) runs { (_, mid) =>
      println("Mid is: " + mid)

      List.empty //no new signals emitted
    }
    fsm1.execute()


    //TODO boxing, SignalID object creation, List object creation takes most time

    //TODO parametrically define what signals we may produce + generate those fields in the state object (maybe
    // (so no return-object (e.g Tuple) needs to be created)

    //with a state
    val fsm2 = fsmWithState(Array[Double](0.0, 0.0)) using(mid("USD"), bid("GBP")) runs { (state, midValue, bidValue) =>
      //println("Mid+bid is: "+(midValue+bidValue))
      //setting state
      state(0) = state(0) + midValue
      state(1) = state(1) + 1
      List(avg(mid("USD")) <-- (state(0) / state(1))) //we emit a new signal + new state
    }


    val RUNS = 10000000
    println("Hit a key...")
    StdIn.readLine()

    for (i <- 1 to 3) {

      time {
        for (i <- 1 to RUNS) {
          fsm2.execute()
        }
      } match {
        case (nanos, _) => {
          println("avg:" + (fsm2.s(0) / fsm2.s(1)))
          val oneRunTime = (nanos.asInstanceOf[Double]) / RUNS
          println("%,.2f".format((1000000000) / oneRunTime) + " exec/sec")
        }
      }

    }

    println("Hit a key...")
    StdIn.readLine()
  }

}

