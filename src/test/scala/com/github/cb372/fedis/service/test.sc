
class C{
  var acc=0
  def minc={acc+1}
  def finc={()=>acc+=1}
}

val c=new C
c.minc
c.finc




//抽象类
abstract class Shape{
    def getArea():Int
}

class Circle(r:Int)extends Shape{
  override def getArea( ): Int = {r*r*3}
}

val s=new Circle(2)
s.getArea()

//特质   特质是一些字段和行为的集合，可以扩展或混入（mixin）你的类中。
trait Car{
   val brand:String
}

trait Shiny{
   val shineRefraction:Int
}

class BMW extends Car with Shiny{
  override val brand: String = "BMW"
  override val shineRefraction: Int = 12
}

//类型
trait Cache[K,V]{
   def get(key:K):V
  def put(key:K,value:V)
  def delete(key:K)
  def remove[K](key:K)
}

//apply 方法   当类或对象有一个主要用途的时候，apply 方法为你提供了一个很好的语法糖。

class Foo{}
object FooMaker{
  def apply() = new Foo
}

val newFoo=FooMaker()

//或
class Bar{
  def apply()=0
}

val bar=new Bar

bar()
//单例对象
object Timer{
  var count=0
  def currentCount():Long={
    count +=1
    count
  }
}

Timer.currentCount()

class Bars(foo:String)
object Bars{
  def apply( foo: String ): Bars = new Bars( foo )
}

//函数即对象
object addOne extends Function1[Int,Int]{
  override def apply( v1: Int ): Int =v1+1
}
addOne(1)

class AddOne extends Function1[Int,Int]{
  override def apply( v1: Int ): Int = v1+1
}
val plusOne=new AddOne
plusOne(1)

class AddOnes extends (Int =>Int){
  override def apply( v1: Int ): Int = v1+1
}


//模式匹配  这是scala 中最有用的部分之一
val times=1
times match {
  case 1 =>"one"
  case 2=>"two"
  case _=>"some other number"
}

//使用守卫进行匹配
times match {
  case i if i==1 =>"one"
  case i if i==2 =>"two"
  case _ =>"some other number"
}
//匹配类型
def bigger(o:Any):Any={
   o match {
     case i:Int if i<0 =>i-1
     case i:Int =>i+1
     case d:Double if d<0.0 =>d-0.1
     case d:Double =>d+0.0
     case text:String=>text +"s"
   }
}

case class Calculator(brand:String,model:String)

//匹配类成员
def calcType(calc:Calculator)=calc match {
  case _ if calc.brand=="hp" && calc.model=="20B" =>"financial"
  case _ if calc.brand=="hp" && calc.model=="48B" =>"scientific"
  case _ if calc.brand=="hp" && calc.model=="30B" =>"business"
  case _ =>"unknown"
}
//样本类case class 使用样本类可以方便得存储匹配类的内容,你不用new 关键字就可以创建它们
val hp20b=Calculator("hp","20B")

val hp20B=Calculator("hp","20B")

hp20b==hp20B

def calcTypeT(calc:Calculator)=calc match {
  case Calculator("hp","20B")=>"financial"
  case Calculator("hp","48G")=>"scientific"
  case Calculator("hp","30B")=>"business"
  case Calculator(ourBrand,ourModel)=>"Calculator:%s %s is of unknow type".format(ourBrand,ourModel)
  case Calculator(_,_)=>"Calculator of unknown type"
  case c@Calculator(_,_) =>"Calculator :%s of unknown type".format(c)
}

//异常











