package io.getquill

import io.getquill.norm.TranspileConfig

case class IdiomContext(config: TranspileConfig, queryType: IdiomContext.QueryType) {
  def traceConfig = config.traceConfig
}

object IdiomContext {
  def Empty = IdiomContext(TranspileConfig.Empty, QueryType.Insert)
  sealed trait QueryType
  object QueryType {
    case object Select extends Regular
    case object Insert extends Regular
    case object Update extends Regular
    case object Delete extends Regular

    case class BatchInsert(foreachAlias: String) extends Batch
    case class BatchUpdate(foreachAlias: String) extends Batch

    sealed trait Regular extends QueryType
    sealed trait Batch extends QueryType

    object Regular {
      def unapply(qt: QueryType): Boolean =
        qt match {
          case r: Regular => true
          case _          => false
        }
    }

    object Batch {
      def unapply(qt: QueryType) =
        qt match {
          case BatchInsert(foreachAlias) => Some(foreachAlias)
          case BatchUpdate(foreachAlias) => Some(foreachAlias)
          case _                         => None
        }
    }
  }
}