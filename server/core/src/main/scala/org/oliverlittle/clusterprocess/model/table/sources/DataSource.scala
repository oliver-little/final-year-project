package org.oliverlittle.clusterprocess.model.table.sources

import org.oliverlittle.clusterprocess.model.table.field.{TableField, TableValue}

trait DataSource:
    /**
      * Abstract implementation to get the headers of a data source
      *
      * @return
      */
    def getHeaders : Map[String, TableField]
    /**
      * Abstract implementation to get data from a data source
      *
      * @return An iterator of rows, each row being a map from field name to a table value
      */
    def getData : Iterable[Map[String, TableValue]]