package com.oracle.oci.dataflow

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.functions.{count, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Date

/**
 * Use case:
 *   This application is aggregation all late responded calls by department or agency
 *   +--------------------+------------------+
 *   |              Agency|LateRespondedCalls|
 *   +--------------------+------------------+
 *   |                NYPD|               873|
 *   |                 DCA|                 3|
 *   +--------------------+------------------+
 *
 * Datasets:
 *   This sample application is based on  open data from NYC 311 service calls.
 *   https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9/data
 *
 * Dependencies:
 *    1. "Apache common lang3" for date, time utilities
 *    2. "scopt" to parse command line options
 *
 */
object Nyc311ServiceCallAggregator extends App {
  case class CommandLineOptions(input: String = null,
                                output: String = null)

  val parser = new scopt.OptionParser[CommandLineOptions]("scopt") {
    head("Provide all required input parameters - ")
    opt[String]('f', "input") required() action { (x, c) =>
      c.copy(input = x)
    } text ("input is the input path")
    opt[String]('o', "output") required() action { (x, c) =>
      c.copy(output = x)
    } text ("output is the output path")
  }

  val commandLineOptions = parser.parse(args, CommandLineOptions()).getOrElse {
    throw new IllegalArgumentException
  }

  val spark = SparkSession.builder()
  // .appName("NYC 311 service calls aggregation app")
  //  .config("spark.master", "local")
    .getOrCreate()


  import spark.implicits._

  val postalBoundaries = spark.read
    .option("header", "true")
    .csv(commandLineOptions.input)

  def parseDate(date: String) = {
    DateUtils.parseDate(date, "mm/dd/yyyy HH:mm:ss a")
  }

  val isRespondedLate = udf((createdDate: String, closedDate: String) => {
    val from = parseDate(createdDate)
    val to =  if(StringUtils.isEmpty(closedDate)){
                     new Date() // Right now
               } else {
                   parseDate(closedDate)
               }

    if (DateUtils.isSameDay(from, to)) {
      val millis = to.getTime - from.getTime
      if (millis * 0.001 > 60 * 60) {
        true
      } else {
        false
      }
    } else {
      true
    }
  })

  val resultDf = postalBoundaries
    .filter(isRespondedLate($"Created Date", $"Closed Date"))
    .groupBy("Agency")
    .agg(count("*").as("LateRespondedCalls"))
    .orderBy($"LateRespondedCalls".desc)

  resultDf.show()
  
  resultDf
    .coalesce(1)
    .write
    .option("header", "true")
    .mode(SaveMode.Overwrite)
    .csv(commandLineOptions.output)
}
