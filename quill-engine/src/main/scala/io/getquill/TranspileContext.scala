package io.getquill

import io.getquill.norm.TranspileConfig

case class IdiomContext(config: TranspileConfig, queryType: IdiomContext.QueryType) {
  def traceConfig = config.traceConfig
}

object IdiomContext {
  def Empty = IdiomContext(TranspileConfig.Empty, QueryType.Regular)
  sealed trait QueryType
  object QueryType {
    case object Regular extends QueryType
    case class Batch(foreachAlias: String) extends QueryType
  }
}