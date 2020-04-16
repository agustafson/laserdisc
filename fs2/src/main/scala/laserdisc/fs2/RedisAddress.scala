package laserdisc
package fs2

import java.net.InetSocketAddress

import cats.{ApplicativeError, Eq, Order}
import cats.instances.int.catsKernelStdOrderForInt
import cats.instances.string.catsKernelStdOrderForString
import cats.instances.tuple.catsKernelStdOrderForTuple2
import cats.syntax.eq._

final case class RedisAddress(host: Host, port: Port) {
  def toInetSocketAddress[F[_]](implicit F: ApplicativeError[F, Throwable]): F[InetSocketAddress] = F.catchNonFatal {
    new InetSocketAddress(host.value, port.value)
  }
  override def toString: String = host.value + ":" + port.value
}

object RedisAddress {
  implicit final val redisAddressEq: Eq[RedisAddress] = Eq.instance { (a1, a2) =>
    a1.host.value === a2.host.value && a1.port.value === a2.port.value
  }

  implicit final val portOrder: Order[Port] = Order.by(_.value)

  implicit final val redisAddressOrder: Order[RedisAddress] =
    Order.by(redisAddress => (redisAddress.host.value, redisAddress.port))
}
