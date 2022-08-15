package io.getquill

import io.getquill.norm.TranspileConfig

case class IdiomContext(config: TranspileConfig, batchAlias: Option[String]) {
  def traceConfig = config.traceConfig
}

object IdiomContext {
  def Empty = IdiomContext(TranspileConfig.Empty, None)
}