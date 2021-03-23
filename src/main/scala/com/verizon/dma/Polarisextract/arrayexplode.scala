package com.verizon.dma.Polarisextract
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{lit, udf}

object arrayexplode {


  // UDF to extract i-th element from array column
  val elem = udf((x: Seq[Int], y: Int) => x(y))

  // Method to apply 'elem' UDF on each element, requires knowing length of sequence in advance
  def splitt(col: Column, len: Int): Seq[Column] = {
    for (i <- 0 until len) yield { elem(col, lit(i)).as(s"$col($i)") }
  }

  // Implicit conversion to make things nicer to use, e.g.
  // select(Column, Seq[Column], Column) is converted into select(Column*) flattening sequences
  implicit class DataFrameSupport(df: DataFrame) {
    def select(cols: Any*): DataFrame = {
      var buffer: Seq[Column] = Seq.empty
      for (col <- cols) {
        if (col.isInstanceOf[Seq[_]]) {
          buffer = buffer ++ col.asInstanceOf[Seq[Column]]
        } else {
          buffer = buffer :+ col.asInstanceOf[Column]
        }
      }
      df.select(buffer:_*)
    }
  }
}
