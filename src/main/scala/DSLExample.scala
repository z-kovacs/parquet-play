import java.time.Duration
import java.time.temporal.ChronoUnit

import scala.collection.mutable
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

  implicit def ps[A](signalExprT: SignalExpr[A]) = ProducedSignals1(signalExprT)
  implicit def ps[A, B](signalExprT: (SignalExpr[A], SignalExpr[B])) = ProducedSignals2(signalExprT._1, signalExprT._2)

}

sealed trait SignalExpr[T] {
  def <--(v: T) = SignalAssignment[T](this, v)
}

case class Mid(ccy: String) extends SignalExpr[Double]

case class Bid(ccy: String) extends SignalExpr[Double]

case class Avg(signal: SignalExpr[Double]) extends SignalExpr[Double]

case class SignalAssignment[T](signalExpr: SignalExpr[T], value: T) {

}


case class EmptyFSM2[S](s: S) {
  def using[T](signalExpr: SignalExpr[T]): Args1[S, T] = Args1(s, signalExpr)
  def using[T, Y](signalExpr: SignalExpr[T], signalExpr2: SignalExpr[Y]): Args2[S, T, Y] = Args2(s, signalExpr, signalExpr2)

  //TODO more tuples?

}

trait ProducedSignals {
  type ProducedSignalTypes
}

case class ProducedSignals1[A](signalExpr: SignalExpr[A]) extends ProducedSignals{
  type ProducedSignalTypes = Tuple1[A]
}

case class ProducedSignals2[A, B](signalExpr: SignalExpr[A], signalExpr2: SignalExpr[B]) extends ProducedSignals{
  type ProducedSignalTypes = Tuple2[A, B]
}

trait Args {
  type InputArgs
  def signalTypeList: Seq[SignalExpr[_]]
  //reverse tuple transformer, from a dynamic list to a tuple
  def signalInputArgsTransformer(inputArgs:Array[_]):InputArgs
}

abstract class AbstractArgs[S,ARGTYPE <: AbstractArgs[_,_]](s:S) extends Args{
  def producing[A](prodSignals: ProducedSignals1[A]) = ArgsAndProducedSignals(s, this.asInstanceOf[ARGTYPE], prodSignals)
  def producing[A,B](prodSignals: ProducedSignals2[A,B]) = ArgsAndProducedSignals(s, this.asInstanceOf[ARGTYPE], prodSignals)

  //TODO more produced
  //TODO maybe use tuples directly in input, and do wrapping here? (no implicit defs)

}

case class Args1[S, T](s: S, signalExpr: SignalExpr[T]) extends AbstractArgs[S,Args1[S,T]](s) {

  type InputArgs = Tuple1[T]
  def signalTypeList: Seq[SignalExpr[_]]=Array(signalExpr)

  def signalInputArgsTransformer(x:Array[_]) = Tuple1(x(0).asInstanceOf[T])

  def runs(fun: (S, T) => List[SignalAssignment[_]]) = FSMWithInPlaceState[S](s, List(signalExpr), (s: S, l: Seq[_]) => fun(s, l(0).asInstanceOf[T]))
}

case class Args2[S, T, Y](s: S, signalExpr: SignalExpr[T], signalExpr2: SignalExpr[Y]) extends AbstractArgs[S,Args2[S,T,Y]](s) {

  type InputArgs = (T, Y)

  def signalTypeList: Seq[SignalExpr[_]]=Array(signalExpr, signalExpr2)
  def signalInputArgsTransformer(x:Array[_]) = Tuple2(x(0).asInstanceOf[T],x(0).asInstanceOf[Y])

  def runs(fun: (S, T, Y) => List[SignalAssignment[_]]) = FSMWithInPlaceState[S](s, List(signalExpr, signalExpr2), (s: S, l: Seq[_]) => fun(s, l(0).asInstanceOf[T], l(1).asInstanceOf[Y]))
}

case class ArgsAndProducedSignals[S, A <: Args,PS <: ProducedSignals](s: S, in: A, out: PS) {
  type producedSignalTypes = out.ProducedSignalTypes
  type inTypes = in.InputArgs

  def running(f: (S,inTypes) => producedSignalTypes) = NewFSM(s, in.signalTypeList,in, out, { (s:S, inputArgsAsList:Array[_]) =>
    f(s,in.signalInputArgsTransformer(inputArgsAsList)).asInstanceOf[Product].productIterator
  })

}

object NewFSM{
  val Observations:Map[SignalExpr[_],Double]=Map(
    Mid("USD") -> 0.1,
    Bid("USD") -> 0.2,
    Bid("GBP") -> 0.2)
}

case class NewFSM[S, PS <: ProducedSignals, A <: Args, X, Y](state: S, list: Seq[SignalExpr[_]],in: A, out: PS, func: (S,Array[_]) => Y) {
  type producedSignalTypes = out.ProducedSignalTypes
  type inTypes = in.InputArgs


  def resolve(signalExpr: SignalExpr[_]): Any =
    NewFSM.Observations(signalExpr)

  def dependecies = in
  def outputs = out


  val resolverFuncs:Array[() => Any]=list.toArray.map( signalExpr => () => resolve(signalExpr) )
  val nrOfInputs:Int=resolverFuncs.size
  val results:Array[Any]=new Array(resolverFuncs.size)

  def execute() = {
    //resolve signals, ad then call

    var i:Int=0
    while(i < nrOfInputs -1 /*includes state*/) {
      results(i)=resolverFuncs(i)()
      i+=1
    }

    val signalValues=func(state, results)
    //TODO signalvalues could be various tuples
    /*val out=signalValues match {
      case (a) => "("+a+")"
      case (a,b) => "("+a+","+b+")"

    }
    println(out)*/
  }

}



//pro: in advance we knoew deps
//pro: typesafe construcitons
//pro: in code we can do any maths magic
//con: generated signals are not known pre-run => we cannot statically build an execution tree
case class FSMWithInPlaceState[T](state: T, list: List[SignalExpr[_]], func: (T, Seq[_]) => List[SignalAssignment[_]]) {

  def resolve(signalExpr: SignalExpr[_]): Any =
    NewFSM.Observations(signalExpr)

  def dependecies = list
  //def outputs = out


  val resolverFuncs:Array[() => Any]=list.toArray.map( signalExpr => () => resolve(signalExpr) )
  val nrOfInputs:Int=resolverFuncs.size
  val results:mutable.MutableList[Any]=new mutable.MutableList[Any]()
  //resize it
  resolverFuncs.foreach( _ => results+= null )


  def execute(): List[SignalAssignment[_]] = {
    //resolve signals, ad then call

    var i:Int=0
    while(i < nrOfInputs ) {
      results(i)=resolverFuncs(i)()
      i+=1
    }
    func(state, results)
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
    val fsm2 = fsmWithState(Array[Double](0.0, 0.0)) using (mid("USD"), bid("GBP")) runs { (state, midValue, bidValue) =>
      //println("Mid+bid is: "+(midValue+bidValue))
      //setting state
      state(0) = state(0) + midValue
      state(1) = state(1) + 1
      List(avg(mid("USD")) <-- (state(0) / state(1))) //we emit a new signal + new state
    }

    val fsm3 = fsmWithState(Array[Double](0.0, 0.0))
      .using (mid("USD"), bid("GBP"))
      .producing (avg(mid("USD")))
      .running { case (state,(midValue,bidValue)) =>
        state(0) = state(0) + midValue
        state(1) = state(1) + 1
        Tuple1(state(0) / state(1))
      }



    val RUNS = 100000000


    val toTest= fsm3
    println(toTest.dependecies)
    //println(toTest.outputs)

    println("Hit a key...")
    StdIn.readLine()

    for (i <- 1 to 3) {

      time {
        var i=0
        while (i < RUNS) {
          toTest.execute
          i+=1
        }
      } match {
        case (nanos, _) => {
          println("avg:" + (toTest.state(0) / toTest.state(1)))
          val oneRunTime = (nanos.asInstanceOf[Double]) / RUNS
          println("%,.2f".format((1000000000) / oneRunTime) + " exec/sec")
        }
      }

    }

    println("Hit a key...")
    StdIn.readLine()
  }

}

