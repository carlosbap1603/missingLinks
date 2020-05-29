package fr.lri.wikipedia

import org.apache.spark.sql.DataFrame

trait AvroWriter {
  def writeAvro(df:DataFrame, outputPath:String, coalesce:Boolean = false, mode:String = "overwrite") = {
    val df_tmp = coalesce match {
      case true => df.coalesce(1)
      case false => df
    }

    df_tmp.write.option("delimiter", "\t")
                .mode(mode)
                .format("avro")
                .save(s"${outputPath}")
  }
}