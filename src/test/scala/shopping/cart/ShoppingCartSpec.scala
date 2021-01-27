package shopping.cart

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.pattern.StatusReply
import akka.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpecLike

object ShoppingCartSpec {
  val config = ConfigFactory
    .parseString("""
      akka.actor.serialization-bindings {
        "shopping.cart.CborSerializable" = jackson-cbor
      }
      """)
    .withFallback(EventSourcedBehaviorTestKit.config)
}

class ShoppingCartSpec
    extends ScalaTestWithActorTestKit(ShoppingCartSpec.config)
    with AnyWordSpecLike
    with BeforeAndAfterEach {

  private val cartId = "testCart"
  private val eventSourcedTestKit =
    EventSourcedBehaviorTestKit[
      ShoppingCart.Command,
      ShoppingCart.State,
      ShoppingCart.State](system, ShoppingCart(cartId, "carts-0"))

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    eventSourcedTestKit.clear()
  }

  "The Shopping Cart" should {

    "add item" in {
      val result1 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          replyTo => ShoppingCart.AddItem("foo", 42, replyTo))
      result1.reply should ===(
        StatusReply.Success(
          ShoppingCart.Summary(Map("foo" -> 42), checkedOut = false)))
      result1.event should ===(ShoppingCart.State(cartId, Map("foo"-> 42), None))
    }

    "reject already added item" in {
      val result1 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          ShoppingCart.AddItem("foo", 42, _))
      result1.reply.isSuccess should ===(true)
      val result2 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          ShoppingCart.AddItem("foo", 13, _))
      result2.reply.isError should ===(true)
    }

    "remove item" in {
      val result1 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          ShoppingCart.AddItem("foo", 42, _))
      result1.reply.isSuccess should ===(true)
      val result2 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          ShoppingCart.RemoveItem("foo", _))
      result2.reply should ===(
        StatusReply.Success(
          ShoppingCart.Summary(Map.empty, checkedOut = false)))
      result2.event should ===(ShoppingCart.State(cartId, Map.empty[String, Int], None))
    }

    "adjust quantity" in {
      val result1 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          ShoppingCart.AddItem("foo", 42, _))
      result1.reply.isSuccess should ===(true)
      val result2 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          ShoppingCart.AdjustItemQuantity("foo", 43, _))
      result2.reply should ===(
        StatusReply.Success(
          ShoppingCart.Summary(Map("foo" -> 43), checkedOut = false)))
      result2.event should ===(
        ShoppingCart.State(cartId, Map("foo"-> 43), None))
    }


    "checkout" in {
      val result1 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          ShoppingCart.AddItem("foo", 42, _))
      result1.reply.isSuccess should ===(true)
      val result2 = eventSourcedTestKit
        .runCommand[StatusReply[ShoppingCart.Summary]](ShoppingCart.Checkout(_))
      result2.reply should ===(
        StatusReply.Success(
          ShoppingCart.Summary(Map("foo" -> 42), checkedOut = true)))
      val state = result2.event.asInstanceOf[ShoppingCart.State]
      state.cartId should ===(cartId)
      state.checkoutDate.isDefined should ===(true)

      val result3 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          ShoppingCart.AddItem("bar", 13, _))
      result3.reply.isError should ===(true)
    }



    "get" in {
      val result1 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          ShoppingCart.AddItem("foo", 42, _))
      result1.reply.isSuccess should ===(true)

      val result2 = eventSourcedTestKit.runCommand[ShoppingCart.Summary](
        ShoppingCart.Get(_))
      result2.reply should ===(
        ShoppingCart.Summary(Map("foo" -> 42), checkedOut = false))
    }


    "keep its state" in {
      val result1 =
        eventSourcedTestKit.runCommand[StatusReply[ShoppingCart.Summary]](
          ShoppingCart.AddItem("foo", 42, _))
      result1.reply should ===(
        StatusReply.Success(
          ShoppingCart.Summary(Map("foo" -> 42), checkedOut = false)))

      eventSourcedTestKit.restart()

      val result2 = eventSourcedTestKit.runCommand[ShoppingCart.Summary](
        ShoppingCart.Get(_))
      result2.reply should ===(
        ShoppingCart.Summary(Map("foo" -> 42), checkedOut = false))
    }
  }

}
