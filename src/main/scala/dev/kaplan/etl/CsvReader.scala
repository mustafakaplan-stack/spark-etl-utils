package dev.kaplan.etl

/**
 * CSV reader with configurable schema inference,
 * null handling, and data type coercion.
 *
 * {{{
 * val reader = CsvReader.builder()
 *   .delimiter(",")
 *   .header(true)
 *   .nullValues(Seq("NA", "null", ""))
 *   .inferSchema(true)
 *   .build()
 *
 * val df = reader.read(spark, "data/sales.csv")
 * }}}
 */
case class CsvReaderConfig(
  delimiter: String = ",",
  header: Boolean = true,
  inferSchema: Boolean = true,
  nullValues: Seq[String] = Seq("", "null", "NA", "N/A"),
  dateFormat: String = "yyyy-MM-dd",
  timestampFormat: String = "yyyy-MM-dd'T'HH:mm:ss",
  maxColumns: Int = 2048,
  encoding: String = "UTF-8",
  quote: Char = '"',
  escape: Char = '\\',
  multiLine: Boolean = false,
)

object CsvReader {

  class Builder {
    private var config = CsvReaderConfig()

    def delimiter(d: String): Builder = { config = config.copy(delimiter = d); this }
    def header(h: Boolean): Builder = { config = config.copy(header = h); this }
    def inferSchema(i: Boolean): Builder = { config = config.copy(inferSchema = i); this }
    def nullValues(nv: Seq[String]): Builder = { config = config.copy(nullValues = nv); this }
    def dateFormat(f: String): Builder = { config = config.copy(dateFormat = f); this }
    def timestampFormat(f: String): Builder = { config = config.copy(timestampFormat = f); this }
    def encoding(e: String): Builder = { config = config.copy(encoding = e); this }
    def multiLine(m: Boolean): Builder = { config = config.copy(multiLine = m); this }

    def build(): CsvReaderConfig = config
  }

  def builder(): Builder = new Builder()

  /**
   * Parse a single CSV line respecting quotes and escapes.
   *
   * @param line    the raw CSV line
   * @param config  reader configuration
   * @return        sequence of field values
   */
  def parseLine(line: String, config: CsvReaderConfig = CsvReaderConfig()): Seq[String] = {
    val result = scala.collection.mutable.ArrayBuffer[String]()
    val current = new StringBuilder()
    var inQuotes = false
    var i = 0

    while (i < line.length) {
      val c = line(i)

      if (c == config.escape && i + 1 < line.length) {
        current.append(line(i + 1))
        i += 2
      } else if (c == config.quote) {
        inQuotes = !inQuotes
        i += 1
      } else if (c.toString == config.delimiter && !inQuotes) {
        result += normalizeNull(current.toString().trim, config)
        current.clear()
        i += 1
      } else {
        current.append(c)
        i += 1
      }
    }

    result += normalizeNull(current.toString().trim, config)
    result.toSeq
  }

  /**
   * Replace configured null-like values with empty string.
   */
  private def normalizeNull(value: String, config: CsvReaderConfig): String = {
    if (config.nullValues.contains(value)) "" else value
  }

  /**
   * Infer the data type of a string value.
   *
   * @return one of: "integer", "double", "boolean", "date", "string"
   */
  def inferType(value: String): String = {
    if (value.isEmpty) return "string"

    // Try integer
    if (value.matches("-?\\d+") && value.length <= 18) return "integer"

    // Try double
    if (value.matches("-?\\d+\\.\\d+")) return "double"

    // Try boolean
    if (Set("true", "false").contains(value.toLowerCase)) return "boolean"

    // Try date (yyyy-MM-dd)
    if (value.matches("\\d{4}-\\d{2}-\\d{2}")) return "date"

    "string"
  }
}
