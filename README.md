# 🧮 spark-etl-utils

A utility library for Apache Spark ETL pipelines with schema evolution, data quality checks, and change data capture (CDC) support.

[![Maven Central](https://img.shields.io/badge/maven-v0.6.0-C71A36)](https://search.maven.org)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Scala 2.13](https://img.shields.io/badge/Scala-2.13-DC322F.svg)](https://scala-lang.org)

## Features

- 📐 **Schema evolution** — Automatic schema migration and compatibility checks
- ✅ **Data quality** — Declarative data quality rules with reporting
- 🔄 **CDC support** — Change data capture with merge and upsert operations
- 📊 **Metrics** — Pipeline execution metrics and data profiling
- 🧩 **Composable** — Functional API for building reusable transforms

## Quick Start

```scala
// build.sbt
libraryDependencies += "dev.mustafakaplan" %% "spark-etl-utils" % "0.6.0"
```

```scala
import dev.mustafakaplan.etl._
import dev.mustafakaplan.etl.quality._
import dev.mustafakaplan.etl.cdc._

// Define a quality-checked pipeline
val pipeline = Pipeline("user_ingestion")
  .extract(
    Source.parquet("s3://data-lake/raw/users/")
      .withSchemaEvolution(SchemaEvolution.Additive)
  )
  .transform(
    Transforms.chain(
      Transforms.deduplicate("user_id"),
      Transforms.fillNulls(Map("country" -> "Unknown")),
      Transforms.castTypes(Map("age" -> "integer", "created_at" -> "timestamp")),
    )
  )
  .qualityCheck(
    QualityRules()
      .notNull("user_id", "email")
      .unique("user_id")
      .range("age", 13, 120)
      .regex("email", "^[\\w.]+@[\\w.]+\\.[a-z]{2,}$")
      .freshness("created_at", hours = 48)
  )
  .load(
    Sink.delta("s3://data-lake/curated/users/")
      .withCDC(CDCConfig(
        keys = Seq("user_id"),
        strategy = MergeStrategy.UpsertWithSoftDelete,
        timestampCol = "updated_at"
      ))
  )

// Execute and get metrics
val result = pipeline.run(spark)
println(s"Processed: ${result.recordsProcessed}")
println(s"Quality score: ${result.qualityScore}%")
println(s"New: ${result.cdc.inserts}, Updated: ${result.cdc.updates}")
```

## License

Apache License 2.0
