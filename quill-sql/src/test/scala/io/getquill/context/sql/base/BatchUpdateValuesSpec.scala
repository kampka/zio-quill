package io.getquill.context.sql.base

import io.getquill.base.Spec
import io.getquill.context.sql.SqlContext
import org.scalatest.BeforeAndAfterEach

trait BatchUpdateValuesSpec extends Spec with BeforeAndAfterEach {

  val context: SqlContext[_, _]
  import context._

  case class ContactBase(firstName: String, lastName: String, age: Int)
  val dataBase: List[ContactBase] = List(
    ContactBase("Joe", "Bloggs", 22),
    ContactBase("A", "A", 111),
    ContactBase("B", "B", 111),
    ContactBase("Joe", "Roggs", 33),
    ContactBase("C", "C", 111),
    ContactBase("D", "D", 111),
    ContactBase("Jim", "Jones", 44),
    ContactBase("Jim", "Domes", 55),
    ContactBase("Caboose", "Castle", 66),
    ContactBase("E", "E", 111)
  )
  def includeInUpdate(c: ContactBase) = c.firstName == "Joe" || c.firstName == "Jim" || c.firstName == "Caboose"
  val updateBase = dataBase.filter(includeInUpdate(_))
  val expectBase = dataBase.map { r =>
    if (includeInUpdate(r)) r.copy(lastName = r.lastName + "U") else r
  }

  trait Adaptable {
    type Row
    def makeData(c: ContactBase): Row
    implicit class AdaptOps(list: List[ContactBase]) {
      def adapt: List[Row] = list.map(makeData(_))
    }
    lazy val update = dataBase.adapt
    lazy val expect = expectBase.adapt
    lazy val data = dataBase.adapt
  }

  object `Ex 1 - Simple Contact` extends Adaptable {
    case class Contact(firstName: String, lastName: String, age: Int)
    type Row = Contact
    override def makeData(c: ContactBase): Contact = Contact(c.firstName, c.lastName, c.age)

    val insert = quote {
      liftQuery(data).foreach(ps => query[Contact].insertValue(ps))
    }
    val q = quote {
      liftQuery(update).foreach(ps =>
        query[Contact].filter(p => p.firstName == ps.firstName).updateValue(ps))
    }
    val get = quote(query[Contact])
  }

  object `Ex 2 - Optional Embedded with Renames` extends Adaptable {
    case class Name(first: String, last: String) extends Embedded
    case class ContactTable(name: Option[Name], age: Int)
    type Row = ContactTable
    override def makeData(c: ContactBase): ContactTable = ContactTable(Some(Name(c.firstName, c.lastName)), c.age)

    val contacts = quote {
      querySchema[ContactTable]("Contact", _.name.map(_.first) -> "firstName", _.name.map(_.last) -> "lastName")
    }

    val insert = quote {
      liftQuery(data).foreach(ps => query[ContactTable].insertValue(ps))
    }
    val q = quote {
      liftQuery(update).foreach(ps =>
        contacts
          .filter(p => p.name.map(_.first) == ps.name.map(_.first))
          .update(_.name.map(_.last) -> ps.name.map(_.last)))
    }
    val get = quote(query[ContactTable])
  }

  object `Ex 3 - Deep Embedded Optional` extends Adaptable {
    case class FirstName(firstName: Option[String]) extends Embedded
    case class LastName(lastName: Option[String]) extends Embedded
    case class Name(first: FirstName, last: LastName) extends Embedded
    case class Contact(name: Option[Name], age: Int)
    type Row = Contact
    override def makeData(c: ContactBase): Contact = Contact(Some(Name(FirstName(Option(c.firstName)), LastName(Option(c.lastName)))), c.age)

    val insert = quote {
      liftQuery(data).foreach(ps => query[Contact].insertValue(ps))
    }
    val q = quote {
      liftQuery(update).foreach(ps =>
        query[Contact]
          .filter(p => p.name.flatMap(_.first.firstName) == ps.name.flatMap(_.first.firstName))
          .update(_.name.flatMap(_.last.lastName) -> ps.name.flatMap(_.last.lastName)))
    }
    val get = quote(query[Contact])
  }
}