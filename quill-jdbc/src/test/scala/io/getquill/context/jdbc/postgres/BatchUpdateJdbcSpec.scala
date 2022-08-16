package io.getquill.context.jdbc.postgres

import io.getquill.context.sql.base.BatchUpdateValuesSpec

class BatchUpdateValuesJdbcSpec extends BatchUpdateValuesSpec {

  val context = testContext
  import testContext._

  override def beforeEach(): Unit = {
    testContext.run(query[ContactBase].delete)
    super.beforeEach()
  }

  "Ex 1 - Simple Contact" in {
    import `Ex 1 - Simple Contact`._
    context.run(insert)
    context.run(update)
    context.run(get) mustEqual (expect)
  }

  "Ex 2 - Optional Embedded with Renames" in {
    import `Ex 2 - Optional Embedded with Renames`._
    context.run(insert)
    context.run(update)
    context.run(get) mustEqual (expect)
  }

  "Ex 3 - Deep Embedded Optional" in {
    import `Ex 3 - Deep Embedded Optional`._
    context.run(insert)
    context.run(update)
    context.run(get) mustEqual (expect)
  }
}
