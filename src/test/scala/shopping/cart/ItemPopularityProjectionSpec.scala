package shopping.cart

import java.time.Instant
import scala.concurrent.{ ExecutionContext, Future }
import akka.Done
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.query.Offset
import akka.projection.ProjectionId
import akka.projection.eventsourced.EventEnvelope
import akka.projection.scaladsl.Handler
import akka.projection.testkit.scaladsl.TestProjection
import akka.projection.testkit.scaladsl.TestSourceProvider
import akka.projection.testkit.scaladsl.ProjectionTestKit
import akka.stream.scaladsl.Source
import org.scalatest.wordspec.AnyWordSpecLike
import shopping.cart.repository.{ ItemPopularityRepository, ScalikeJdbcSession }

object ItemPopularityProjectionSpec {
  // stub out the db layer and simulate recording item count updates
  class TestItemPopularityRepository extends ItemPopularityRepository {
    var counts: Map[String, Long] = Map.empty

    override def update(
        session: ScalikeJdbcSession,
        itemId: String,
        delta: Int): Unit = {
      counts = counts + (itemId -> (counts.getOrElse(itemId, 0L) + delta))
    }

    override def getItem(
        session: ScalikeJdbcSession,
        itemId: String): Option[Long] =
      counts.get(itemId)
  }
}

class ItemPopularityProjectionSpec
    extends ScalaTestWithActorTestKit
    with AnyWordSpecLike {
  import ItemPopularityProjectionSpec.TestItemPopularityRepository

  private val projectionTestKit = ProjectionTestKit(system)

  private def createEnvelope(
      event: ShoppingCart.State,
      seqNo: Long,
      timestamp: Long = 0L) =
    EventEnvelope(
      Offset.sequence(seqNo),
      "persistenceId",
      seqNo,
      event,
      timestamp)

  private def toAsyncHandler(itemHandler: ItemPopularityProjectionHandler)(
      implicit
      ec: ExecutionContext): Handler[EventEnvelope[ShoppingCart.State]] =
    eventEnvelope =>
      Future {
        itemHandler.process(session = null, eventEnvelope)
        Done
      }

  "The events from the Shopping Cart" should {

    "update item popularity counts by the projection" in {

      val cartId789 = "a7098"
      val cartId123 = "0d12d"
      val events =
        Source(
          List[EventEnvelope[ShoppingCart.State]](
            createEnvelope(
              ShoppingCart.State(cartId789, Map("bowling shoes"->1), None),
              0L),
            createEnvelope(
              ShoppingCart.State(cartId789, Map("bowling shoes"->2), None),
              1L),
            createEnvelope(
              ShoppingCart
                .State(cartId789,Map("bowling shoes"->2), Some( Instant.parse("2020-01-01T12:00:00.00Z"))),
              2L),
            createEnvelope(
              ShoppingCart.State(cartId123, Map("akka t-shirt"->1), None),
              3L),
            createEnvelope(ShoppingCart.State(cartId123, Map("akka t-shirt"->1, "skis"-> 1), None), 4L),
            createEnvelope(ShoppingCart.State(cartId123,Map("akka t-shirt"->1), None), 5L),
            createEnvelope(
              ShoppingCart
                .State(cartId123,Map("akka t-shirt"->1), Some( Instant.parse("2020-01-01T12:05:00.00Z"))),
              6L)))

      val repository = new TestItemPopularityRepository
      val projectionId =
        ProjectionId("item-popularity", "carts-0")
      val sourceProvider =
        TestSourceProvider[Offset, EventEnvelope[ShoppingCart.State]](
          events,
          extractOffset = env => env.offset)
      val projection =
        TestProjection[Offset, EventEnvelope[ShoppingCart.State]](
          projectionId,
          sourceProvider,
          () =>
            toAsyncHandler(
              new ItemPopularityProjectionHandler(
                "carts-0",
                system,
                repository))(system.executionContext))

      projectionTestKit.run(projection) {
        repository.counts shouldBe Map(
          "bowling shoes" -> 2,
          "akka t-shirt" -> 1)
      }
    }
  }

}
