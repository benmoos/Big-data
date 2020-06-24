object HelloYou extends App{

  def add(o1: Option[Int], o2: Option[Int]): Option[Int] = (o1,o2) match {
    case (Some(o1),Some(o2)) => Some(o1+o2)
    case (Some(o1),None) => Some(o1)
    case (None,Some(o2)) => Some(o2)
    case (None,None) => None
  }

  def add2(o1: Option[Int], o2: Option[Int]): Option[Int] = {
    (o1 ++ o2).reduceOption(_ + _)
  }

  def add3(o1: Option[Int], o2: Option[Int]): Option[Int] = {
    var acc=0
    for (option <- o1 ++ o2 ) acc += option
    if (acc==0) None
    else Some(acc)
  }




  def average(values: Iterator[Double]): Double= {
    val (sum,length)=values.foldLeft((0.0,0.0)) { case ((s,l),x)=> (s+x,l+1) }
    sum/length



  }

  //print(average(Iterator(10.0,20.0,30.0,40.0,50.0)))


 sealed trait Maybe[+A] {
    def map[B](f: A => B): Maybe[B]={
      this match {
        case Yep(a)=> Yep(f(a))
        case Nope => Nope
      }
    }
    def flatMap[B](f: A => Maybe[B]): Maybe[B]={
      this match {
        case Yep(a)=> f(a)
        case Nope => Nope
      }
    }
  }

/* sealed trait Maybe[+A] {
   def map[B](f: A => B): Maybe[B]= {
     this match {
       case Yep(a)=> flatMap(f)
       case Nope => Nope
     }
   }



   def flatMap[B](f: A => Maybe[B]): Maybe[B]={
     this match {
       case Yep(a)=> f(a)
       case Nope => Nope
     }
   }
 }*/
 case class Yep[A](value: A) extends Maybe[A]
 case object Nope extends Maybe[Nothing]

 println(for { a <- Yep(1); b <- Nope: Maybe[Int] } yield a + b)


 sealed trait Expression
 case class Const(value: Double) extends Expression
 case class Add(left: Expression, right: Expression) extends Expression
 case class Mult(left: Expression, right: Expression) extends Expression



 def eval(exp: Expression): Double = exp match {
   case Add(Const(x),Const(y))=> x+y
   case Mult(Const(x),Const(y))=> x*y
   case Mult(Const(x),_)=> eval(_)*x
 }

  print(eval(Mult(Const(2), Add(Const(1), Const(3)))) == 8)
}
