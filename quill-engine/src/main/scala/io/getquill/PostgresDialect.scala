package io.getquill

import java.util.concurrent.atomic.AtomicInteger
import io.getquill.ast.{ Action, _ }
import io.getquill.ast
import io.getquill.context.sql.idiom.SqlIdiom.{ InsertUpdateStmt, copyIdiom }
import io.getquill.context.{ CanInsertReturningWithMultiValues, CanInsertWithMultiValues, CanReturnClause }
import io.getquill.context.sql.idiom._
import io.getquill.idiom.{ Statement, Token, ValuesClauseToken }
import io.getquill.idiom.StatementInterpolator._
import io.getquill.norm.ProductAggregationToken
import io.getquill.util.Messages.fail

trait PostgresDialect
  extends SqlIdiom
  with QuestionMarkBindVariables
  with ConcatSupport
  with OnConflictSupport
  with CanReturnClause
  with CanInsertWithMultiValues
  with CanInsertReturningWithMultiValues {

  override protected def productAggregationToken: ProductAggregationToken = ProductAggregationToken.VariableDotStar

  override def astTokenizer(implicit astTokenizer: Tokenizer[Ast], strategy: NamingStrategy, transpileContext: TranspileContext): Tokenizer[Ast] =
    Tokenizer[Ast] {
      case ListContains(ast, body) => stmt"${body.token} = ANY(${ast.token})"
      case c: OnConflict           => conflictTokenizer.token(c)
      case ast                     => super.astTokenizer.token(ast)
    }

  override implicit def operationTokenizer(implicit astTokenizer: Tokenizer[Ast], strategy: NamingStrategy): Tokenizer[Operation] =
    Tokenizer[Operation] {
      case UnaryOperation(StringOperator.`toLong`, ast) => stmt"${scopedTokenizer(ast)}::bigint"
      case UnaryOperation(StringOperator.`toInt`, ast)  => stmt"${scopedTokenizer(ast)}::integer"
      case operation                                    => super.operationTokenizer.token(operation)
    }

  private[getquill] val preparedStatementId = new AtomicInteger

  override def prepareForProbing(string: String) = {
    var i = 0
    val query = string.flatMap(x => if (x != '?') s"$x" else {
      i += 1
      s"$$$i"
    })
    s"PREPARE p${preparedStatementId.incrementAndGet.toString.token} AS $query"
  }

  override protected def actionTokenizer(insertEntityTokenizer: Tokenizer[Entity])(implicit astTokenizer: Tokenizer[Ast], strategy: NamingStrategy, transpileContext: TranspileContext): Tokenizer[ast.Action] =
    Tokenizer[ast.Action] {
      // always need to have action-alias present because using FROM-values clause
      // TODO Shouldn't need any of this if non-batch queries are being used, can use regular UPDATE for that, should look into that optimization
      case action @ Update(Filter(_: Entity, alias, _), _) =>
        PostgresDialectExt.updateWithValues(this, action, alias)

      case other =>
        super.actionTokenizer(insertEntityTokenizer).token(other)
    }
}

object PostgresDialect extends PostgresDialect

object PostgresDialectExt {
  //case class UpdateWithValues(action: Statement, where: Statement)
  private[getquill] def updateWithValues(parentIdiom: SqlIdiom, action: ast.Action, alias: Ident)(implicit strategy: NamingStrategy, transpileContext: TranspileContext): Statement = {
    val idiom = copyIdiom(parentIdiom, Some(alias))
    import idiom._

    implicit val stableTokenizer = idiom.astTokenizer(new Tokenizer[Ast] {
      override def token(v: Ast): Token = astTokenizer(this, strategy, transpileContext).token(v)
    }, strategy, transpileContext)

    //    UPDATE people AS p SET id = p.id, name = p.name, age = p.age
    //    FROM (values (1, 'Joe', 111), (2, 'Jack', 222))
    //    AS c(id, name, age)
    //    WHERE c.id = p.id

    // Uses the `alias` passed in as `actionAlias` since that is now assigned to the copied SqlIdiom
    action match {
      case Update(Filter(table: Entity, x, where), assignments) =>
        val (columns, values) = columnsAndValues(assignments)
        val columnSetters =
          stmt"UPDATE ${table.token}${` AS [table]`} SET FROM (VALUES ${ValuesClauseToken(stmt"(${values.mkStmt(", ")})")}) ${assignments.token} ${where.token}"

        null

      case Update(table: Entity, assignments) =>
        stmt"UPDATE ${table.token}${` AS [table]`} SET ${assignments.token}"

      case _ =>
        fail("Invalid state. Only UPDATE/DELETE with filter allowed here.")
    }
  }
}
