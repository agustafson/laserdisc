package laserdisc.fs2

import cats.data.NonEmptySet
import cats.effect.concurrent.Ref
import cats.effect.Sync
import cats.implicits._

sealed trait RedisConnection
final case class RedisSingleNodeConnection(redisAddress: RedisAddress) extends RedisConnection
sealed abstract case class RedisClusterConnection[F[_]](redisAddresses: Ref[F, NonEmptySet[RedisAddress]]) extends RedisConnection {
//  def getAddress:
}
object RedisClusterConnection {
  def apply[F[_]: Sync](redisAddresses: NonEmptySet[RedisAddress]): F[RedisClusterConnection[F]] = {
    Ref[F].of(redisAddresses).map(ref => new RedisClusterConnection[F](ref) {})
  }
}
