package shopping.cart

import java.time.Instant

import scala.concurrent.duration._

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityContext

import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.pattern.StatusReply
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplyEffect
import akka.persistence.typed.scaladsl.RetentionCriteria

object ShoppingCart {

  final case class State(cartId: String, items: Map[String, Int], checkoutDate: Option[Instant])
    extends CborSerializable {

    def isCheckedOut: Boolean =
      checkoutDate.isDefined

    def hasItem(itemId: String): Boolean =
      items.contains(itemId)

    def isEmpty: Boolean =
      items.isEmpty

    def updateItem(itemId: String, quantity: Int): State = {
      quantity match {
        case 0 => copy(items = items - itemId)
        case _ => copy(items = items + (itemId -> quantity))
      }
    }

    def removeItem(itemId: String): State =
      copy(items = items - itemId)

    def checkout(now: Instant): State =
      copy(checkoutDate = Some(now))

    def toSummary: Summary =
      Summary(items, isCheckedOut)
  }

  object State {
    def empty(cartId:String) =
      State(cartId = cartId, items = Map.empty, checkoutDate = None)
  }

  sealed trait Command extends CborSerializable
  final case class AddItem(
                            itemId: String,
                            quantity: Int,
                            replyTo: ActorRef[StatusReply[Summary]])
    extends Command
  final case class RemoveItem(
                               itemId: String,
                               replyTo: ActorRef[StatusReply[Summary]])
    extends Command
  final case class AdjustItemQuantity(
                                       itemId: String,
                                       quantity: Int,
                                       replyTo: ActorRef[StatusReply[Summary]])
    extends Command
  final case class Checkout(replyTo: ActorRef[StatusReply[Summary]])
    extends Command
  final case class Get(replyTo: ActorRef[Summary]) extends Command
  final case class Summary(items: Map[String, Int], checkedOut: Boolean)
    extends CborSerializable



  val EntityKey: EntityTypeKey[Command] =
    EntityTypeKey[Command]("ShoppingCart")


  val tags = Vector.tabulate(5)(i => s"carts-$i")

  def init(system: ActorSystem[_]): Unit = {
    val behaviorFactory: EntityContext[Command] => Behavior[Command] = {
      entityContext =>
        val i = math.abs(entityContext.entityId.hashCode % tags.size)
        val selectedTag = tags(i)
        ShoppingCart(entityContext.entityId, selectedTag)
    }
    ClusterSharding(system).init(Entity(EntityKey)(behaviorFactory))
  }

  def apply(cartId: String, projectionTag: String): Behavior[Command] = {
    EventSourcedBehavior
      .withEnforcedReplies[Command, State, State](
        persistenceId = PersistenceId(EntityKey.name, cartId),
        emptyState = State.empty(cartId),
        commandHandler =
          (state, command) => handleCommand(cartId, state, command),
        eventHandler = (state, event) => handleEvent(state, event))
      .withTagger(_ => Set(projectionTag)) // <1>
      .withRetention(RetentionCriteria
        .snapshotEvery(numberOfEvents = 100, keepNSnapshots = 3))
      .onPersistFailure(
        SupervisorStrategy.restartWithBackoff(200.millis, 5.seconds, 0.1))
  }

  private def handleCommand(
                             cartId: String,
                             state: State,
                             command: Command): ReplyEffect[State, State] = {
    // The shopping cart behavior changes if it's checked out or not.
    // The commands are handled differently for each case.
    if (state.isCheckedOut)
      checkedOutShoppingCart(cartId, state, command)
    else
      openShoppingCart(cartId, state, command)
  }


  private def openShoppingCart(
                                cartId: String,
                                state: State,
                                command: Command): ReplyEffect[State, State] = {
    command match {
      case AddItem(itemId, quantity, replyTo) =>
        if (state.hasItem(itemId))
          Effect.reply(replyTo)(
            StatusReply.Error(
              s"Item '$itemId' was already added to this shopping cart"))
        else if (quantity <= 0)
          Effect.reply(replyTo)(
            StatusReply.Error("Quantity must be greater than zero"))
        else
          Effect
            .persist(state.updateItem( itemId, quantity))
            .thenReply(replyTo) { updatedCart =>
              StatusReply.Success(updatedCart.toSummary)
            }

      case RemoveItem(itemId, replyTo) =>
        if (state.hasItem(itemId))
          Effect
            .persist(state.removeItem( itemId))
            .thenReply(replyTo)(updatedCart =>
              StatusReply.Success(updatedCart.toSummary))
        else
          Effect.reply(replyTo)(
            StatusReply.Success(state.toSummary)
          ) // removing an item is idempotent

      case AdjustItemQuantity(itemId, quantity, replyTo) =>
        if (quantity <= 0)
          Effect.reply(replyTo)(
            StatusReply.Error("Quantity must be greater than zero"))
        else if (state.hasItem(itemId))
          Effect
            .persist(
              state.updateItem(itemId, quantity))
            .thenReply(replyTo)(updatedCart =>
              StatusReply.Success(updatedCart.toSummary))
        else
          Effect.reply(replyTo)(StatusReply.Error(
            s"Cannot adjust quantity for item '$itemId'. Item not present on cart"))


      case Checkout(replyTo) =>
        if (state.isEmpty)
          Effect.reply(replyTo)(
            StatusReply.Error("Cannot checkout an empty shopping cart"))
        else
          Effect
            .persist(state.checkout(Instant.now()))
            .thenReply(replyTo)(updatedCart =>
              StatusReply.Success(updatedCart.toSummary))


      case Get(replyTo) =>
        Effect.reply(replyTo)(state.toSummary)


    }
  }

  private def checkedOutShoppingCart(
                                      cartId: String,
                                      state: State,
                                      command: Command): ReplyEffect[State, State] = {
    command match {
      case Get(replyTo) =>
        Effect.reply(replyTo)(state.toSummary)
      case cmd: AddItem =>
        Effect.reply(cmd.replyTo)(
          StatusReply.Error(
            "Can't add an item to an already checked out shopping cart"))
      case cmd: RemoveItem =>
        Effect.reply(cmd.replyTo)(
          StatusReply.Error(
            "Can't remove an item from an already checked out shopping cart"))
      case cmd: AdjustItemQuantity =>
        Effect.reply(cmd.replyTo)(
          StatusReply.Error(
            "Can't adjust item on an already checked out shopping cart"))
      case cmd: Checkout =>
        Effect.reply(cmd.replyTo)(
          StatusReply.Error("Can't checkout already checked out shopping cart"))
    }
  }

  private def handleEvent(oldState: State, newState: State): State = newState

}
