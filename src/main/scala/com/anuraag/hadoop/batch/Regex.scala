package com.anuraag.hadoop.batch

import java.util.UUID

import scala.util.matching.Regex

object Regex {
  def regexTest() : Unit = {
    val keyValPattern: Regex = "([0-9a-zA-Z- ]+): ([0-9a-zA-Z-#()/. ]+)".r
    val newKeyValPattern: Regex = """/home/anuraag/Downloads/stackexchange/christianity.meta.stackexchange.com/([a-zA-Z]+).xml""".r

    val newInput: String = "Badges.xml"
    val input: String =
      """background-color: #A03300;
        |background-image: url(img/header100.png);
        |background-position: top center;
        |background-repeat: repeat-x;
        |background-size: 2160px 108px;
        |margin: 0;
        |height: 108px;
        |width: 100%;""".stripMargin

    for (patternMatch <- newKeyValPattern.findAllMatchIn(newInput))
      println(s"key: ${patternMatch.group(1)} value: ${patternMatch.group(2)}")

  }

  def regexDate(): Unit = {
    val year_month_day_hour = """(\d{4})-(\d{2})-(\d{2})-(\d{2})""".r

    val fileName = "Findings-" + UUID.randomUUID() + "-2019-12-13-12.gz"
    println("Filename: " + fileName)

    val extractedDate = year_month_day_hour.findFirstIn(fileName).get
    println(s"Extracted Date: $extractedDate")

    val modifiedDate = extractedDate.replace('-', '/')
    println(s"Modified Date: $modifiedDate")
  }

  def regexGeneral() : Unit = {
    val d1 = "720-870-9532"
    val d2 = "887.922.9498"
    val d3 = "213*986*9659"

    val pattern = "[0-9][-.]\\d\\d\\d[-.]\\d\\d\\d\\d".r
    val mat = pattern.findFirstIn(d1)
    println(pattern.findFirstIn(d1))
    println(pattern.findFirstIn(d2))
    println(pattern.findFirstIn(d3))
  }

}
