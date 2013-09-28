package models

import quickfix.Message
import quickfix.SessionID
import quickfix.MessageCracker
import quickfix.fix42._
import akka.actor._
import quickfix.SessionSettings
import java.io._
import quickfix.FileStoreFactory
import quickfix.FileLogFactory
import quickfix.DefaultMessageFactory
import quickfix.SocketAcceptor
import quickfix.field._
import quickfix.Session

class Fix(self: ActorRef, market: ActorRef) extends MessageCracker with quickfix.Application {
  var execId = System.currentTimeMillis()
  private def nextExecId(): Long = {
    execId += 1
    execId
  }

  def fromAdmin(msg: Message, sessionId: SessionID): Unit = println(s"FAdm $msg")
  def fromApp(msg: Message, sessionId: SessionID): Unit = {
    println(s"FApp $msg")
    crack(msg, sessionId)
  }
  def onCreate(sessionId: SessionID): Unit = println(s"CS: $sessionId")
  def onLogon(sessionId: SessionID): Unit = println(s"LOn: $sessionId")
  def onLogout(sessionId: SessionID): Unit = println(s"LOff: $sessionId")
  def toAdmin(msg: Message, sessionId: SessionID): Unit = println(s"TAdm $msg")
  def toApp(msg: Message, sessionId: SessionID): Unit = println(s"TApp $msg")

  def onMessage(msg: NewOrderSingle, sessionId: SessionID) = {
    val symbol = new Symbol
    msg.get(symbol)
    val clOrdId = new ClOrdID
    msg.get(clOrdId)
    val side = new Side
    msg.get(side)
    val price = new Price
    msg.get(price)
    val quantity = new OrderQty
    msg.get(quantity)

    val o = Order(sessionId, Some(clOrdId.getValue), clOrdId.getValue, symbol.getValue, side.getValue() == Side.BUY, price.getValue, quantity.getValue.toLong)
    reportExecution(o, Amount(0, 0), new OrdStatus(OrdStatus.NEW), new ExecType(ExecType.NEW))

    market.tell(MarketBySymbolMessage.Add(o), self)
  }

  def onMessage(msg: OrderCancelRequest, sessionId: SessionID) = {
    val symbol = new Symbol
    msg.get(symbol)
    val clOrdId = new ClOrdID
    msg.get(clOrdId)
    val side = new Side
    msg.get(side)

    val o = Order(sessionId, None, clOrdId.getValue, symbol.getValue, side.getValue() == Side.BUY, 0.0, 0)

    market.tell(MarketBySymbolMessage.Delete(o), self)
  }

  def onMessage(msg: OrderCancelReplaceRequest, sessionId: SessionID) = {
    val symbol = new Symbol
    msg.get(symbol)
    val origClOrdId = new OrigClOrdID
    msg.get(origClOrdId)
    val clOrdId = new ClOrdID
    msg.get(clOrdId)
    val side = new Side
    msg.get(side)
    val price = new Price
    msg.get(price)
    val quantity = new OrderQty
    msg.get(quantity)

    val oldO = Order(sessionId, None, origClOrdId.getValue, symbol.getValue, side.getValue() == Side.BUY, 0.0, 0)
    val newO = Order(sessionId, None, clOrdId.getValue, symbol.getValue, side.getValue() == Side.BUY, price.getValue, quantity.getValue.toLong)

    market.tell(MarketBySymbolMessage.Update(oldO, newO), self)
  }

  def reportExecution(o: Order, a: Amount, status: OrdStatus, execType: ExecType) = {
    val er = new ExecutionReport
    er.set(new OrderID(o.id.get))
    er.set(new ClOrdID(o.clId))
    er.set(new ExecID(nextExecId().toString))
    er.set(status)
    er.set(new ExecTransType(ExecTransType.NEW))
    er.set(execType)
    er.set(new Symbol(o.symbol))
    er.set(new LeavesQty(o.quantity - a.quantity))
    er.set(new CumQty(a.quantity))

    println(er)
    Session.sendToTarget(er, o.sessionId.getSenderCompID, o.sessionId.getTargetCompID)
  }
}

class FixActor(market: ActorRef, settingsFile: String) extends Actor {
  val fixApp = new Fix(self, market)
  val settings = new SessionSettings(new FileInputStream(settingsFile))
  val storeFactory = new FileStoreFactory(settings)
  val logFactory = new FileLogFactory(settings)
  val messageFactory = new DefaultMessageFactory()
  val acceptor = new SocketAcceptor(fixApp, storeFactory, settings, logFactory, messageFactory)
  acceptor.start()

  def receive = {
    case u: Updates =>
      println(s"FIX $u")
      val execs = u.mbo.execs
      for (exec <- execs) {
        report(exec.s1)
        report(exec.s2)
        def report(level: Level[Order]) = {
          val leavesAmount = Amount(level.a.price, level.a.quantity - exec.a.quantity)
          val filled = leavesAmount.quantity == 0
          fixApp.reportExecution(level.t, level.a, new OrdStatus(if (filled) OrdStatus.FILLED else OrdStatus.PARTIALLY_FILLED), new ExecType(if (filled) ExecType.FILL else ExecType.PARTIAL_FILL))
        }
      }
  }
}

object Fix extends App {
  val system = ActorSystem()
  val market = system.actorOf(Props[MarketBySymbolActor], "Market")
  system.actorOf(Props(classOf[FixActor], market, "settings.txt"))
}