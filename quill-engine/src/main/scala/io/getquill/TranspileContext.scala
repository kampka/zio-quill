package io.getquill

import io.getquill.ast.{ Ast, CollectAst }
import io.getquill.norm.TranspileConfig
import io.getquill.ast

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

    def discoverFromAst(theAst: Ast): QueryType = {
      // If it's a batch-query it has a foreach, get the alias so we can create the QueryType.BatchInsert/Update
      val batchAlias =
        CollectAst(theAst) {
          case ast.Foreach(_, alias, _) => alias.name
        }.headOption

      val actions =
        CollectAst(theAst) {
          case _: ast.Insert => QueryType.Insert
          case _: ast.Update => QueryType.Update
          case _: ast.Delete => QueryType.Delete
        }
      if (actions.length > 1) println(s"[WARN] Found more then one type of Query: ${actions}. Using 1st one!")
      // if we have not found it to specifically be an action, it must just be a regular select query
      val resultType: QueryType.Regular = actions.headOption.getOrElse(QueryType.Select)
      resultType match {
        case QueryType.Insert => batchAlias.map(QueryType.BatchInsert(_)) getOrElse QueryType.Insert
        case QueryType.Update => batchAlias.map(QueryType.BatchUpdate(_)) getOrElse QueryType.Update
        case QueryType.Delete => Delete
        case QueryType.Select => Select
      }
    }
  }
}