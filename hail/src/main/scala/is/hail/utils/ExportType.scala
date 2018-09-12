package is.hail.utils

object ExportType {
  val CONCATENATED = 0
  val PARALLEL_SEPARATE_HEADER = 1
  val PARALLEL_HEADER_IN_SHARD = 2

  def getExportType(typ: String): Int = {
    typ match {
      case null => CONCATENATED
      case "separate_header" => PARALLEL_SEPARATE_HEADER
      case "header_per_shard" => PARALLEL_HEADER_IN_SHARD
      case _ => fatal(s"Unknown export type: `$typ'")
    }
  }
}
