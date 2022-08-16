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
    ContactBase("Jan", "Roggs", 33),
    ContactBase("C", "C", 111),
    ContactBase("D", "D", 111),
    ContactBase("James", "Jones", 44),
    ContactBase("Dale", "Domes", 55),
    ContactBase("Caboose", "Castle", 66),
    ContactBase("E", "E", 111)
  )
  def includeInUpdate(c: ContactBase) = List("Joe", "Jan", "James", "Dale", "Caboose").contains(c.firstName)
  val updateBase =
    dataBase.filter(includeInUpdate(_)).map(r => r.copy(lastName = r.lastName + "U"))
  val expectBase = dataBase.map { r =>
    if (includeInUpdate(r)) r.copy(lastName = r.lastName + "U") else r
  }

  trait Adaptable {
    type Row
    def makeData(c: ContactBase): Row
    implicit class AdaptOps(list: List[ContactBase]) {
      def adapt: List[Row] = list.map(makeData(_))
    }
    lazy val updateData = updateBase.adapt
    lazy val expect = expectBase.adapt
    lazy val data = dataBase.adapt
  }

  object `Ex 1 - Simple Contact` extends Adaptable {
    case class Contact(firstName: String, lastName: String, age: Int)
    type Row = Contact
    override def makeData(c: ContactBase): Contact = Contact(c.firstName, c.lastName, c.age)

    val insert = quote {
      liftQuery(data: List[Contact]).foreach(ps => query[Contact].insertValue(ps))
    }
    val update = quote {
      liftQuery(updateData: List[Contact]).foreach(ps =>
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
      liftQuery(data: List[ContactTable]).foreach(ps => contacts.insertValue(ps))
    }
    val update = quote {
      liftQuery(updateData: List[ContactTable]).foreach(ps =>
        contacts
          .filter(p => p.name.map(_.first) == ps.name.map(_.first))
          .update(_.name.map(_.last) -> ps.name.map(_.last)))
    }
    val get = quote(contacts)
  }

  object `Ex 3 - Deep Embedded Optional` extends Adaptable {
    case class FirstName(firstName: Option[String]) extends Embedded
    case class LastName(lastName: Option[String]) extends Embedded
    case class Name(first: FirstName, last: LastName) extends Embedded
    case class Contact(name: Option[Name], age: Int)
    type Row = Contact
    override def makeData(c: ContactBase): Contact = Contact(Some(Name(FirstName(Option(c.firstName)), LastName(Option(c.lastName)))), c.age)

    val insert = quote {
      liftQuery(data: List[Contact]).foreach(ps => query[Contact].insertValue(ps))
    }
    val update = quote {
      liftQuery(updateData: List[Contact]).foreach(ps =>
        query[Contact]
          .filter(p => p.name.map(_.first.firstName) == ps.name.map(_.first.firstName))
          .update(_.name.map(_.last.lastName) -> ps.name.map(_.last.lastName)))
    }
    val get = quote(query[Contact])
  }
}