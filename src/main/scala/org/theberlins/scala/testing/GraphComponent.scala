package org.theberlins.scala.testing

import java.util.UUID

trait GraphComponent {
	val uuid = UUID.randomUUID.toString
}
