package io.getquill

import io.getquill.norm.TranspileConfig

case class TranspileContext(config: TranspileConfig, batchAlias: Option[String]) {
  def traceConfig = config.traceConfig
}

object TranspileContext {
  def Empty = TranspileContext(TranspileConfig.Empty, None)
}