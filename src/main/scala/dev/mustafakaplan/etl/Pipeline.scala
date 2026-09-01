package dev.mustafakaplan.etl

import org.apache.spark.sql.{DataFrame, SparkSession}

case class PipelineResult(
  recordsProcessed: Long,
  qualityScore: Double,
  durationMs: Long
)

class Pipeline(name: String)(implicit spark: SparkSession) {

  private var extractFn: Option[() => DataFrame] = None
  private var transforms: List[DataFrame => DataFrame] = Nil
  private var loadFn: Option[DataFrame => Long] = None
  private var qualityRules: Option[QualityRules] = None

  def extract(fn: () => DataFrame): Pipeline = {
    extractFn = Some(fn)
    this
  }

  def transform(fn: DataFrame => DataFrame): Pipeline = {
    transforms = transforms :+ fn
    this
  }

  def qualityCheck(rules: QualityRules): Pipeline = {
    qualityRules = Some(rules)
    this
  }

  def load(fn: DataFrame => Long): Pipeline = {
    loadFn = Some(fn)
    this
  }

  def run(): PipelineResult = {
    val start = System.currentTimeMillis()

    println(s"▶ Pipeline '$name' starting")

    // Extract
    var df = extractFn.getOrElse(
      throw new IllegalStateException("No extract function defined")
    )()

    println(s"  [extract] ${df.count()} records loaded")

    // Transform
    transforms.zipWithIndex.foreach { case (fn, i) =>
      df = fn(df)
      println(s"  [transform ${i + 1}] ${df.count()} records")
    }

    // Quality check
    val score = qualityRules.map(_.evaluate(df)).getOrElse(100.0)
    println(f"  [quality] score: $score%.1f%%")

    // Load
    val written = loadFn.map(_(df)).getOrElse(df.count())
    val duration = System.currentTimeMillis() - start

    println(s"✔ Pipeline '$name' completed in ${duration}ms ($written records written)")

    PipelineResult(written, score, duration)
  }
}
