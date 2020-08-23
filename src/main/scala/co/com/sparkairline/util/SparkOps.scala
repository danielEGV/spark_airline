package co.com.sparkairline.util

import org.apache.spark.sql.Dataset

object SparkOps {
  implicit class DataSetOps[A](ds: Dataset[A]) {

    def filterOpt[B](opt: Option[B])(f: (A, B) => Boolean): Dataset[A] = if (opt.isDefined) ds.filter(f(_, opt.get)) else ds

  }
}
