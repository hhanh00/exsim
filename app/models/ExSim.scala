package models

import akka.actor._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import quickfix.SessionID

case class Amount(price: Double, quantity: Long)
case class Order(sessionId: SessionID, id: Option[String], clId: String, symbol: String, buy: Boolean, price: Double, quantity: Long) extends HasAmount {
  def toAmount = Amount(price, quantity)
  val aggregate = false
  
  override def hashCode = id.hashCode
  override def equals(o: Any): Boolean = o match {
    case other: Order => clId == other.clId
    case _ => false
  }
}
case class Insert[A](i: Int, a: A)
case class Delete[A](i: Int, len: Int, a: A)
case class Update[A](i: Int, a: A)

trait HasAmount {
  def toAmount: Amount
  val aggregate: Boolean
}

trait MOut
case class Trade[T](a: Amount, s1: Level[T], s2: Level[T])
case class Level[T](b: Boolean, a: Amount, t: T)

case class MultiUpdate[T](insert: Option[Insert[Level[T]]], delete: Option[Delete[Level[T]]], update: Option[Update[Level[T]]], execs: List[Trade[T]]) extends MOut {
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
          val trade = Trade(Amount(p, execQ), l, ol)

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

case class MarketPrice(buy: Option[Amount] = None, sell: Option[Amount] = None, last: Option[Amount] = None) {
  def update(mbp: MarketBy[Null], execs: List[Trade[Order]]): MarketPrice = MarketPrice(mbp.bestBuy, mbp.bestSell, execs.headOption.map(_.a))
  def diff(that: MarketPrice): MarketPrice = MarketPrice(
    if (buy == that.buy) None else that.buy,
    if (sell == that.sell) None else that.sell,
    if (last == that.last) None else that.last)
}

case class Updates(mp: MarketPrice, mbp: MultiUpdate[Null], mbo: MultiUpdate[Order])

class MarketTrades {
  var trades: List[Trade[Order]] = Nil

  override def toString() = s"$trades"
  def add(ts: List[Trade[Order]]) = trades ++= ts
}

class MarketBySymbol(symbol: String) {
  val mbo = new MarketBy[Order](false)
  val mbp = new MarketBy[Null](true)
  var mp = MarketPrice()
  val mt = new MarketTrades

  def add(o: Order): Updates = {
    val mbou = mbo.addNew(o.buy, o.toAmount, o)
    val execs = mbou.execs
    val mbpu = mbp.addNew(o.buy, o.toAmount, null)
    val newMp = mp.update(mbp, execs)
    val diffMp = mp.diff(newMp)
    mp = newMp
    
    mt.add(execs)
    println(mp, mt, execs)
    Updates(diffMp, mbpu, mbou)
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
    case MarketBySymbolMessage.Add(o) => sender ! getOrCreate(o.symbol).add(o)
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
