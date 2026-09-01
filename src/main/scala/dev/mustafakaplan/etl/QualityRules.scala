package dev.mustafakaplan.etl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class QualityRule(name: String, check: DataFrame => Double)

class QualityRules {
  private var rules: List[QualityRule] = Nil

  def notNull(columns: String*): QualityRules = {
    columns.foreach { col =>
      rules = rules :+ QualityRule(
        s"not_null($col)",
        df => {
          val total = df.count().toDouble
          if (total == 0) 100.0
          else {
            val valid = df.filter(df(col).isNotNull).count().toDouble
            (valid / total) * 100.0
          }
        }
      )
    }
    this
  }

  def unique(column: String): QualityRules = {
    rules = rules :+ QualityRule(
      s"unique($column)",
      df => {
        val total = df.count().toDouble
        if (total == 0) 100.0
        else {
          val distinct = df.select(column).distinct().count().toDouble
          (distinct / total) * 100.0
        }
      }
    )
    this
  }

  def evaluate(df: DataFrame): Double = {
    if (rules.isEmpty) return 100.0

    val scores = rules.map { rule =>
      val score = rule.check(df)
      println(f"    • ${rule.name}: $score%.1f%%")
      score
    }
    scores.sum / scores.length
  }
}
