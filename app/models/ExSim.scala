package models

import akka.actor._
import scala.collection.mutable.ArrayBuffer

case class Order(symbol: String, buy: Boolean, price: Double, quantity: Long)
case class Level(symbol: String, buy: Boolean, price: Double, quantity: Long)
case class Insert[A](i: Int, a: A)
case class Delete[A](len: Int, a: A)
case class Update[A](i: Int, a: A, dq: Long)

trait MOut
case class MultiUpdate[A](insert: Option[Insert[A]], delete: Option[Delete[A]], update: Option[Update[A]], execs: List[Order]) extends MOut
case class Command(tpe: CommandTpe.Value, o: Order) extends MOut
object CommandTpe extends Enumeration {
  val New, Amend, Cancel = Value
}

class MarketByPrice(slc: ActorRef, fix: ActorRef) extends Actor {
  val buys = ArrayBuffer.empty[Level]
  val sells = ArrayBuffer.empty[Level]
  def receive = {
    case mu: MultiUpdate[Order] => apply(mu)
  }

  def apply(mu: MultiUpdate[Order]) = {
    println(s"Applying $mu")
    val list = List(
      mu.insert.map { i =>
        (i.a.buy, i.a.price, i.a.quantity)
      },
      mu.update.map { u =>
        (u.a.buy, u.a.price, u.dq)
      },
      mu.delete.map { d =>
        (d.a.buy, d.a.price, -d.a.quantity)
      })
    val dqs = list.flatten.groupBy(e => (e._1, e._2)).map({ case (k, v) => k -> v.map(_._3).sum })
    for (
      (k, dq) <- dqs;
      (b, p) = k
    ) {
      val s = side(b)
      val q = s.find(l => l.price == p).getOrElse()

    }
  }

  private def side(b: Boolean): ArrayBuffer[Level] = {
    if (b)
      buys
    else
      sells
  }
}

class MarketByOrder(mbp: ActorRef) extends Actor {
  val buys = ArrayBuffer.empty[Order]
  val sells = ArrayBuffer.empty[Order]
  def receive = {
    case Command(CommandTpe.New, o) =>
      val mu = newOrder(o, MultiUpdate[Order](None, None, None, Nil))
      apply(mu)
      dump()
    case _ =>
  }

  def dump() = {
    println(buys, sells)
  }

  def apply(mu: MultiUpdate[Order]) = {
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
  }

  private def sides(o: Order): (ArrayBuffer[Order], ArrayBuffer[Order], (Order) => Boolean) = {
    if (o.buy)
      (buys, sells, (order) => order.price <= o.price)
    else
      (sells, buys, (order) => order.price >= o.price)
  }

  private def newOrder(o: Order, mu: MultiUpdate[Order]): MultiUpdate[Order] = {
    val (b, s, cmp) = sides(o)
    val i = b.indexWhere(order => cmp(order))
    val index = if (i < 0) buys.size else i
    val mu2 = mu copy (insert = Some(Insert(index, o)))

    def fill(o: Order, mu: MultiUpdate[Order]): MultiUpdate[Order] = {
      val q = o.quantity
      val p = o.price

      def fillQ(qLeft: Long, index: Int, mu: MultiUpdate[Order]): MultiUpdate[Order] = {
        if (qLeft == 0 || index >= s.size || s(index).price > p)
          mu
        else {
          val oo = s(index)
          val execQ = Math.min(qLeft, oo.quantity)
          val execs = List(o copy (quantity = execQ), oo copy (quantity = execQ))

          if (qLeft < oo.quantity)
            mu copy (insert = None, update = Some(Update(index, oo copy (quantity = oo.quantity - qLeft), -execQ)), execs = execs ++ mu.execs)
          else {
            val d = mu.delete.getOrElse(Delete(0, oo))
            val d2 = d copy (len = d.len + 1)
            val i = mu.insert.get
            val i2 = if (qLeft > oo.quantity) Some(i copy (a = i.a copy (quantity = i.a.quantity - oo.quantity))) else None
            val m2 = mu copy (insert = i2, delete = Some(d2), execs = execs ++ mu.execs)
            fillQ(qLeft - oo.quantity, index + 1, m2)
          }
        }
      }
      fillQ(q, 0, mu)
    }
    fill(o, mu2)
  }
}

class Dummy extends Actor {
  def receive = {
    case x => println(x)
  }
}

object ExSim extends App {
  val system = ActorSystem()
  val slc = system.actorOf(Props(classOf[Dummy]), "SLC")
  val fix = system.actorOf(Props(classOf[Dummy]), "FIX")
  val mbp = system.actorOf(Props(classOf[MarketByPrice], slc, fix), "MBP")
  val mbo = system.actorOf(Props(classOf[MarketByOrder], mbp), "MBO")

  val prices = List(10)
  for (i <- prices) {
    mbo ! Command(CommandTpe.New, Order("HSBC", false, i, 800))
  }
  for (i <- prices) {
    mbo ! Command(CommandTpe.New, Order("HSBC", true, i, 400))
  }

}