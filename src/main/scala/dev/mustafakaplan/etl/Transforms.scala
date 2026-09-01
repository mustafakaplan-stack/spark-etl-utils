package dev.mustafakaplan.etl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Transforms {

  /** Remove duplicate rows based on given columns. */
  def deduplicate(columns: String*): DataFrame => DataFrame = { df =>
    df.dropDuplicates(columns)
  }

  /** Fill null values with specified defaults. */
  def fillNulls(defaults: Map[String, Any]): DataFrame => DataFrame = { df =>
    defaults.foldLeft(df) { case (acc, (col, value)) =>
      acc.na.fill(Map(col -> value))
    }
  }

  /** Chain multiple transforms together. */
  def chain(fns: (DataFrame => DataFrame)*): DataFrame => DataFrame = { df =>
    fns.foldLeft(df) { (acc, fn) => fn(acc) }
  }

  /** Filter rows matching a SQL expression. */
  def filter(condition: String): DataFrame => DataFrame = { df =>
    df.filter(condition)
  }

  /** Rename columns. */
  def rename(mapping: Map[String, String]): DataFrame => DataFrame = { df =>
    mapping.foldLeft(df) { case (acc, (oldName, newName)) =>
      acc.withColumnRenamed(oldName, newName)
    }
  }
}
