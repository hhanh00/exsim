package models

import akka.actor._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

case class Amount(price: Double, quantity: Long)
case class Order(id: String, symbol: String, buy: Boolean, price: Double, quantity: Long) extends HasAmount {
  def toAmount = Amount(price, quantity)
  val aggregate = false
}
case class Insert[A](i: Int, a: A)
case class Delete[A](i: Int, len: Int, a: A)
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
  def isEmpty: Boolean = insert == None && delete == None && update == None
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
      val b = sides(i.a)._1
      b.insert(i.i, i.a)
    }
    mu.update.map { u =>
      val b = sides(u.a)._1
      b(u.i) = u.a
    }
    mu.delete.map { d =>
      val b = sides(d.a)._1
      b.remove(0, d.len)
    }
    mu
  }

  private def sides(l: Level[T]): (ArrayBuffer[Level[T]], ArrayBuffer[Level[T]], (Amount) => Boolean) = {
    if (l.b)
      (buys, sells, (a) => a.price <= l.a.price)
    else
      (sells, buys, (a) => a.price >= l.a.price)
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
            val d = mu.delete.getOrElse(Delete(0, 0, ol))
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

  def cancel(l: Level[T]): MultiUpdate[T] = {
    val (b, _, _) = sides(l)
    val i = b.indexWhere(lvl => lvl.t == l.t)
    if (i >= 0) {
      new MultiUpdate[T] copy (delete = Some(Delete(i, 1, l)))
    } else new MultiUpdate[T]
  }

  def addNew(buy: Boolean, a: Amount, o: T): MultiUpdate[T] = {
    val mu = add(Level[T](buy, a, o), new MultiUpdate[T])
    apply(mu)
    dump()
    mu
  }

  def cancelOld(buy: Boolean, a: Amount, o: T): MultiUpdate[T] = {
    val mu = cancel(Level[T](buy, a, o))
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

  def add(o: Order): Unit = {
    val execs = mbo.addNew(o.buy, o.toAmount, o).execs
    mbp.addNew(o.buy, o.toAmount, null)
    mp.update(mbp, execs)
    mt.add(execs)
    println(mp, mt, execs)
  }

  def cancel(o: Order): Unit = {
    val mu = mbo.cancelOld(o.buy, o.toAmount, o)
    if (!mu.isEmpty) {
      val a = o.toAmount
      val negAmount = a copy (quantity = -a.quantity)
      mbp.addNew(o.buy, negAmount, null)
    }
  }
}

class MarketBySymbolActor extends Actor {
  val ms = HashMap.empty[String, MarketBySymbol]

  def receive = {
    case MarketBySymbolMessage.Add(o) => getOrCreate(o.symbol).add(o)
    case MarketBySymbolMessage.Update(oldO, newO) =>
      getOrCreate(oldO.symbol).cancel(oldO)
      getOrCreate(newO.symbol).add(newO)
    case MarketBySymbolMessage.Delete(o) => getOrCreate(o.symbol).cancel(o)
  }

  private def getOrCreate(s: String) = ms.getOrElseUpdate(s, new MarketBySymbol(s))
}

object MarketBySymbolMessage {
  case class Add(o: Order)
  case class Update(oldO: Order, newO: Order)
  case class Delete(o: Order)
}

object ExSim extends App {
  val system = ActorSystem()
  val market = system.actorOf(Props(classOf[MarketBySymbolActor]), "Market")

  testM(market)

  def testM(m: ActorRef) = {
    val prices = List(10)
    for (i <- prices) {
      m ! MarketBySymbolMessage.Add(Order("S-" + i.toString, "HSBC", false, i, 800))
    }
    for (i <- prices) {
      m ! MarketBySymbolMessage.Add(Order("B-" + i.toString, "HSBC", true, i, 800))
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
      mbo.addNew(false, Amount(i, 800), Order("0", "HSBC", false, i, 800))
      mbo.addNew(false, Amount(i, 800), Order("0", "HSBC", false, i, 800))
      mbo.addNew(false, Amount(i, 800), Order("0", "HSBC", false, i, 800))
    }
    //    for (i <- prices) {
    //      mbo.addNew(true, Order("HSBC", false, i, 1200))
    //    }
  }
  //
}