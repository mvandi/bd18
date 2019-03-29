package it.unibo.bd18.lab4

import java.net.InetAddress

import it.unibo.bd18.app.StreamingApp

trait Lab4Base extends StreamingApp {

  val host = InetAddress.getLocalHost.getHostAddress
  val port = 4012

}
