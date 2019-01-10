package org.senseml.feature

object App {

  def main(args: Array[String]): Unit = {

    run()

  }

  def run(): Unit = {
    val projectPath = App.getClass.getResource("").getPath
    val rootPath = projectPath.substring(projectPath.indexOf("feature"))
    println(rootPath)

    val ordersPath = rootPath + "dataset/orders.txt"



  }

}
