package com.verizon.dma.mail

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame;

object mailutills {
  def showHTML(ds: DataFrame, limit: Int = 40, truncate: Int = 20) = {
    import xml.Utility.escape
    val data = ds.take(limit)
    val header = ds.schema.fieldNames.toSeq
    val rows: Seq[Seq[String]] = data.map { row =>
      row.toSeq.map { cell =>
        val str = cell match {
          case null => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case array: Array[_] => array.mkString("[", ", ", "]")
          case seq: Seq[_] => seq.mkString("[", ", ", "]")
          case _ => cell.toString
        }
        if (truncate > 0 && str.length > truncate) {
          // do not show ellipses for strings shorter than 4 characters.
          if (truncate < 4) str.substring(0, truncate)
          else str.substring(0, truncate - 3) + "..."
        } else {
          str
        }
      }: Seq[String]
    }

    val q = new StringBuilder

    println("insidestringbuilder")

    q.append("<html> <body> <p>Hi team,</p> <p>Please find the details of count mismatch between NBC Apps and Cassandra</p> </body> </html>")
    q.append(System.getProperty("line.separator"))

    println("q before html-" + q)

    q.append(
      s""" <html>
<head>
<style>
table, th, td {
  border: 1px solid black;
  border-collapse: collapse;
}
</style>
</head>
<body>

<table style="width:100%">

                <tr>
                 ${header.map(h => s"<th>${escape(h)}</th>").mkString}
                </tr>
                ${
        rows.map { row =>
          s"<tr>${row.map { c => s"<td>${escape(c)}</td>" }.mkString}</tr>"
        }.mkString
      }
            </table>
        """)
    q.append(System.getProperty("line.separator"))


    println("q after html-" + q)
    q.append("<p>Thanks and Reagrds<br>Track 7 & team</p>")
    q.append(System.getProperty("line.separator"))
    q.append("<p>***********This is an auto generated test POC email by Venkat,Kindly share suggestions if any************</p>")


    println("value final-" + q.toString())
    q.toString()


  }




//
  //  Calling the function
  //  com.verizon.dma.mail.functionExample(showHTML(mail_zero))


}
