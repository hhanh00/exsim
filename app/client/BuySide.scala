package client

import quickfix.Application
import quickfix.Message
import quickfix.SessionID
import quickfix.SessionSettings
import quickfix.FileStoreFactory
import quickfix.FileLogFactory
import quickfix.DefaultMessageFactory
import java.io.FileInputStream
import quickfix.SocketInitiator
import quickfix.fix42.Logon
import quickfix.Session
import quickfix.fix42.NewOrderSingle
import quickfix.field._

class BuySide extends Application {
  def fromAdmin(msg: Message, sessionId: SessionID): Unit = println(msg)
  def fromApp(msg: Message, sessionId: SessionID): Unit = println(msg)
  def onCreate(sessionId: SessionID): Unit = println(s"CS: $sessionId")
  def onLogon(sessionId: SessionID): Unit = println(s"LOn: $sessionId")
  def onLogout(sessionId: SessionID): Unit = println(s"LOff: $sessionId")
  def toAdmin(msg: Message, sessionId: SessionID): Unit = println(msg)
  def toApp(msg: Message, sessionId: SessionID): Unit = println(s"TApp: $msg")

  def newOrderSingle(clOrdId: String, buy: Boolean, price: Double, quantity: Long) = {
    val newOrderSingle = new NewOrderSingle
    newOrderSingle.set(new ClOrdID(clOrdId))
    newOrderSingle.set(new Symbol("HSBC"))
    newOrderSingle.set(new OrderQty(quantity))
    newOrderSingle.set(new Price(price))
    newOrderSingle.set(new Side(if (buy) Side.BUY else Side.SELL))
    Session.sendToTarget(newOrderSingle, "ARCA", "TW")

  }
}

object BuySide extends App {
  val app = new BuySide
  val settings = new SessionSettings(new FileInputStream("client_settings.txt"))
  val storeFactory = new FileStoreFactory(settings)
  val logFactory = new FileLogFactory(settings)
  val messageFactory = new DefaultMessageFactory()
  val initiator = new SocketInitiator(app, storeFactory, settings, logFactory, messageFactory)
  initiator.start()
  val login = new Logon(new EncryptMethod, new HeartBtInt(30))
  Session.sendToTarget(login, "ARCA", "TW")
  Thread.sleep(5000)
  
  app.newOrderSingle("B", true, 100.0, 800)
  //app.newOrderSingle("S", false, 100.0, 800)

  Thread.sleep(5000)
}