package models

import akka.actor._
import scala.collection.mutable.ArrayBuffer

case class Amount(price: Double, quantity: Long)
case class Order(symbol: String, buy: Boolean, price: Double, quantity: Long) extends HasAmount {
  def toAmount = Amount(price, quantity)
  val aggregate = false
}
case class Insert[A](i: Int, a: A)
case class Delete[A](len: Int, a: A)
case class Update[A](i: Int, a: A)

trait HasAmount {
  def toAmount: Amount
  val aggregate: Boolean
}

trait MOut
case class Trade[T](a: Amount, s1: T, s2: T)
case class Level[T](b: Boolean, a: Amount, t: T)

case class MultiUpdate[A](insert: Option[Insert[Level[A]]], delete: Option[Delete[Level[A]]], update: Option[Update[Level[A]]], execs: List[Trade[A]]) extends MOut {
  def this() = this(None, None, None, Nil)
}
case class Command(tpe: CommandTpe.Value, o: Order) extends MOut
object CommandTpe extends Enumeration {
  val New, Amend, Cancel = Value
}

class MarketBy[T <: HasAmount](aggregate: Boolean) {
  val buys = ArrayBuffer.empty[Level[T]]
  val sells = ArrayBuffer.empty[Level[T]]

  def dump() = {
    println(buys, sells)
  }

  def apply(mu: MultiUpdate[T]) = {
    println(s"Applying $mu")
    mu.insert.map { i =>
      val (b, _, _) = sides(i.a)
      b.insert(i.i, i.a)
    }
    mu.update.map { u =>
      val (b, _, _) = sides(u.a)
      b(u.i) = u.a
    }
    mu.delete.map { d =>
      val (b, _, _) = sides(d.a)
      b.remove(0, d.len)
    }
    mu
  }

  private def sides(l: Level[T]): (ArrayBuffer[Level[T]], ArrayBuffer[Level[T]], (Amount) => Boolean) = {
    if (l.b)
      (buys, sells, (order) => order.price <= l.a.price)
    else
      (sells, buys, (order) => order.price >= l.a.price)
  }

  private def add(l: Level[T], mu: MultiUpdate[T]): MultiUpdate[T] = {
    val (b, s, cmp) = sides(l)
    val i = b.indexWhere(level => cmp(level.a))
    val index = if (i < 0) buys.size else i
    val mu2 = if (aggregate && (i >= 0) && b(index).a.price == l.a.price) {
      val muu = mu.update.getOrElse(Update(index, l copy (a = l.a copy (quantity = 0))))
      mu copy (update = Some(muu copy (a = l copy (a = muu.a.a copy (quantity = b(index).a.quantity + l.a.quantity)))))
    } else {
      mu copy (insert = Some(Insert(index, l)))
    }

    def fill(l: Level[T], mu: MultiUpdate[T]): MultiUpdate[T] = {
      val q = l.a.quantity
      val p = l.a.price

      def fillQ(qLeft: Long, index: Int, mu: MultiUpdate[T]): MultiUpdate[T] = {
        if (qLeft == 0 || index >= s.size || s(index).a.price > p)
          mu
        else {
          val ol = s(index)
          val execQ = Math.min(qLeft, ol.a.quantity)
          val trade = Trade(Amount(p, execQ), l.t, ol.t)

          if (qLeft < ol.a.quantity)
            mu copy (insert = None, update = Some(Update(index, ol copy (a = ol.a copy (quantity = ol.a.quantity - qLeft)))), execs = trade :: mu.execs)
          else {
            val d = mu.delete.getOrElse(Delete(0, ol))
            val d2 = d copy (len = d.len + 1)
            val i = mu.insert.get
            val i2 = if (qLeft > ol.a.quantity) Some(i copy (a = i.a copy (a = ol.a copy (quantity = i.a.a.quantity - ol.a.quantity)))) else None
            val m2 = mu copy (insert = i2, delete = Some(d2), execs = trade :: mu.execs)
            fillQ(qLeft - ol.a.quantity, index + 1, m2)
          }
        }
      }
      fillQ(q, 0, mu)
    }
    fill(l, mu2)
  }

  def addNew(buy: Boolean, a: Amount, o: T): MultiUpdate[T] = {
    val mu = add(Level[T](buy, a, o), new MultiUpdate[T])
    apply(mu)
    dump()
    mu
  }

  def bestBuy: Option[Amount] = buys.headOption.map(_.a)
  def bestSell: Option[Amount] = sells.headOption.map(_.a)
}

class Dummy extends Actor {
  def receive = {
    case x => println(x)
  }
}

class MarketPrice {
  var buy, sell, last: Option[Amount] = None
  override def toString() = s"Bid: $buy, Ask: $sell, Last: $last"

  def update(mbp: MarketBy[Null], execs: List[Trade[Order]]) {
    buy = mbp.bestBuy
    sell = mbp.bestSell
    execs.headOption.map(t => last = Some(t.a))
  }
}

class MarketTrades {
  var trades: List[Trade[Order]] = Nil
  
  override def toString() = s"$trades"
  def add(ts: List[Trade[Order]]) = trades ++= ts
}

class MarketBySymbol(symbol: String) {
  val mbo = new MarketBy[Order](false)
  val mbp = new MarketBy[Null](true)
  val mp = new MarketPrice
  val mt = new MarketTrades

  def add(o: Order) = {
    val execs = mbo.addNew(o.buy, o.toAmount, o).execs
    mbp.addNew(o.buy, o.toAmount, null)
    mp.update(mbp, execs)
    mt.add(execs)
    println(mp, mt, execs)
  }
}

object ExSim extends App {
  testM()

  def testM() = {
    val m = new MarketBySymbol("HSBC")
    val prices = List(10)
    for (i <- prices) {
      m.add(Order("HSBC", false, i, 800))
      m.add(Order("HSBC", false, i, 800))
      m.add(Order("HSBC", false, i, 800))
    }
    for (i <- prices) {
      m.add(Order("HSBC", true, i, 800))
      m.add(Order("HSBC", true, i, 800))
      m.add(Order("HSBC", true, i, 800))
    }
  }
  
  def testMP() = {
    val mbo = new MarketBy[Order](true)

  }

  def testMO() = {
    val mbo = new MarketBy[Order](true)

    //  val system = ActorSystem()
    //  val slc = system.actorOf(Props(classOf[Dummy]), "SLC")
    //  val fix = system.actorOf(Props(classOf[Dummy]), "FIX")
    ////  val mbp = system.actorOf(Props(classOf[MarketByPrice], slc, fix), "MBP")
    //  val mbo = system.actorOf(Props(classOf[MarketByOrder[Order]], null), "MBO")
    //
    val prices = List(10)
    for (i <- prices) {
      mbo.addNew(false, Amount(i, 800), Order("HSBC", false, i, 800))
      mbo.addNew(false, Amount(i, 800), Order("HSBC", false, i, 800))
      mbo.addNew(false, Amount(i, 800), Order("HSBC", false, i, 800))
    }
    //    for (i <- prices) {
    //      mbo.addNew(true, Order("HSBC", false, i, 1200))
    //    }
  }
  //
}