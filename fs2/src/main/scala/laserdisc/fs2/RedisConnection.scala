package laserdisc.fs2

import cats.data.NonEmptySet

sealed trait RedisConnection
final case class RedisSingleNodeConnection(redisAddress: RedisAddress)             extends RedisConnection
final case class RedisClusterConnection(redisAddresses: NonEmptySet[RedisAddress]) extends RedisConnection
