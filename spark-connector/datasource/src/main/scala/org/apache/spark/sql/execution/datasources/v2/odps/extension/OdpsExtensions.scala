/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2.odps.extension

import com.aliyun.odps.PartitionSpec

import scala.collection.JavaConverters._
import org.apache.spark.sql.{AnalysisException, Dataset, SparkSession, SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, ResolvedTable, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.catalog.CatalogTable.catalogAndNamespaceToProps
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, Descending, SortOrder, UpCast}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.connector.catalog.LookupCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.odps.{OdpsBucketSpec, OdpsTable, OdpsTableCatalog}
import org.apache.spark.sql.odps.execution.exchange.OdpsShuffleExchangeExec
import org.apache.spark.sql.odps.catalyst.plans.physical.OdpsHashPartitioning
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, PartitioningUtils => CatalystPartitioningUtils}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.types.StructType

import java.util.Locale
import java.lang.reflect.Method
import scala.collection.mutable

class OdpsExtensions extends (SparkSessionExtensions => Unit) {

  private val WRITE_ODPS_STATIC_PARTITION = "writeOdpsStaticPartition"
  private val WRITE_ODPS_DYNAMIC_PARTITION = "writeOdpsDynamicPartitionColumns"

  case class ResolveOdpsView(spark: SparkSession)
    extends Rule[LogicalPlan]
      with LookupCatalog {

    protected lazy val catalogManager = spark.sessionState.catalogManager
    protected lazy val analyzer = spark.sessionState.analyzer

    /**
     * Format table name, taking into account case sensitivity.
     */
    private def formatTableName(name: String): String = {
      if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
    }

    /**
     * Format database name, taking into account case sensitivity.
     */
    private def formatNamespaceName(namespace: Seq[String]): Seq[String] = {
      if (conf.caseSensitiveAnalysis) namespace else namespace.map(_.toLowerCase(Locale.ROOT))
    }

    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
      case u @ UnresolvedRelation(parts @ CatalogAndIdentifier(catalog: OdpsTableCatalog, ident), _, _) =>
        try {
          val view = catalog.loadView(ident)
          createViewRelation(parts, catalog, view)
        } catch {
          case _: Throwable =>
            u
        }
    }

    private def createViewRelation(nameParts: Seq[String], catalog: OdpsTableCatalog, view: OdpsTable): LogicalPlan = {
      val parsedPlan =
        parseViewText(nameParts.toArray.mkString("."), view.viewText.get)
      val aliases = view.schema.zipWithIndex.map {
        case (expected, pos) =>
          val attr = GetColumnByOrdinal(pos, expected.dataType)
          Alias(UpCast(attr, expected.dataType), expected.name)(explicitMetadata =
            Some(expected.metadata))
      }

      val ident = view.tableIdent
      val namespace = formatNamespaceName(ident.namespace())
      val db = namespace.head
      val name = formatTableName(ident.name())
      val viewProperties = catalogAndNamespaceToProps(catalog.name(), namespace)

      val metadata = CatalogTable(
        identifier = TableIdentifier(name, Some(db)),
        tableType = CatalogTableType.VIEW,
        storage = CatalogStorageFormat.empty,
        schema = view.schema,
        properties = viewProperties,
        viewOriginalText = view.viewText,
        viewText = view.viewText
      )
      val subqueryAlias = SubqueryAlias(nameParts, View(desc = metadata, isTempView = false, child = Project(aliases, parsedPlan)))

      val clazz = analyzer.ResolveRelations.getClass
      val method: Method = clazz.getDeclaredMethod(
        "org$apache$spark$sql$catalyst$analysis$Analyzer$ResolveRelations$$resolveViews", classOf[LogicalPlan])
      method.setAccessible(true)
      method.invoke(analyzer.ResolveRelations, subqueryAlias).asInstanceOf[LogicalPlan]
    }

    private def parseViewText(name: String, viewText: String): LogicalPlan = {
      val origin = Origin(
        objectType = Some("VIEW"),
        objectName = Some(name)
      )
      try {
        CurrentOrigin.withOrigin(origin) {
          spark.sessionState.sqlParser.parseQuery(viewText)
        }
      } catch {
        case _: ParseException =>
          throw QueryCompilationErrors.invalidViewText(viewText, name)
      }
    }
  }

  class ResolveOdpsTable(session: SparkSession) extends Rule[LogicalPlan] with SQLConfHelper {

    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {

      case ShowColumns(ResolvedTable(_, _, table: OdpsTable, _), namespace, _) =>
        val resolver = conf.resolver
        namespace match {
          case Some(ns) =>
            val resolvedNamespace = table.tableIdent.namespace()
            val notMatched = resolvedNamespace.length != ns.size ||
              resolvedNamespace.zip(ns).exists {
                case (left, right) => !resolver(left, right)
              }
            if (notMatched) {
              throw new AnalysisException(
                "SHOW COLUMNS with conflicting namespace: " +
                  s"'$ns' != '${resolvedNamespace.mkString("[", ", ", "]")}'")
            }
          case None =>
        }
        ShowColumnsCommand(table)

      case i@InsertIntoStatement(r@DataSourceV2Relation(table: OdpsTable, _, _, _, _), _, _, _, _, _, _)
        if i.query.resolved =>
        if (i.partitionSpec.nonEmpty && !r.options.containsKey(WRITE_ODPS_STATIC_PARTITION)) {
          val normalizedSpec = CatalystPartitioningUtils.normalizePartitionSpec(
            i.partitionSpec,
            table.partitionSchema,
            table.tableIdent.toString,
            session.sessionState.conf.resolver)
          val partitionSpec = normalizedSpec.map {
            case (key, Some(value)) => key + "=" + value
            case (key, _) => key + "=''"
          }.mkString(",")
          val options = r.options.asCaseSensitiveMap().asScala ++
            Map(WRITE_ODPS_STATIC_PARTITION -> partitionSpec)
          i.copy(
            table = r.copy(options = new CaseInsensitiveStringMap(options.asJava)))
        } else {
          i
        }
    }
  }

  class OptimizeWriteOdpsTable(sparkSession: SparkSession) extends Rule[LogicalPlan] {

    private val WRITE_ODPS_TABLE_RESOLVED = "writeOdpsTableResolved"

    private def getDynamicPartition(partitionSpecValue: String,
                                    partitionSchema: StructType): (PartitionSpec, mutable.ArrayBuffer[String]) = {
      var isDynamic = partitionSpecValue.isEmpty
      val odpsStaticPartition = new PartitionSpec
      val dynamicColumns = mutable.ArrayBuffer[String]()

      if (partitionSpecValue.nonEmpty) {

        val partitionSpec = partitionSpecValue.split(",")
          .map(_.split("="))
          .filter(_.length == 2)
          .map(kv => kv(0) -> kv(1).replaceAll("'", "").replaceAll("\"", ""))
          .toMap

        val partitionColumnNames = partitionSchema.fields
          .map(PartitioningUtils.getColName(_, caseSensitive = false))

        var numStaticPartitions = 0

        partitionColumnNames.foreach { field =>
          if (partitionSpec.contains(field) && partitionSpec(field).nonEmpty && !isDynamic) {
            odpsStaticPartition.set(field, partitionSpec(field))
            numStaticPartitions = numStaticPartitions + 1
          } else {
            isDynamic = true
            dynamicColumns += field
          }
        }
      } else {
        partitionSchema.fields.map(PartitioningUtils.getColName(_, caseSensitive = false))
          .foreach { field =>
            dynamicColumns += field
          }
      }

      (odpsStaticPartition, dynamicColumns)
    }

    private def insertRepartition(query: LogicalPlan, table: OdpsTable,
                                  partitionBuckets: Option[Int],
                                  dynamicPartitionColumns: Int): LogicalPlan = {
      table.bucketSpec match {
        case Some(OdpsBucketSpec("hash", numBuckets, bucketColumnNames, sortColumns)) =>
          val bucketAttributes = bucketColumnNames.map(name => {
            query.resolve(name :: Nil, sparkSession.sessionState.analyzer.resolver).getOrElse(
              throw new AnalysisException(
                s"Unable to resolve $name given [${query.output.map(_.name).mkString(", ")}]")
            ).asInstanceOf[Attribute]
          })

          val bucketSortOrders = sortColumns.map(col => {
            val attr = query.resolve(col.name :: Nil, sparkSession.sessionState.analyzer.resolver).getOrElse(
              throw new AnalysisException(
                s"Unable to resolve ${col.name} given [${query.output.map(_.name).mkString(", ")}]")
            ).asInstanceOf[Attribute]
            SortOrder(attr, col.order.toUpperCase() match {
              case "ASC" => Ascending
              case _ => Descending
            })
          })
          val shuffle = OdpsHashRepartition(bucketAttributes, partitionBuckets.getOrElse(numBuckets), query)
          if (sortColumns.nonEmpty || dynamicPartitionColumns > 0) {
            val ordering = if (dynamicPartitionColumns > 0) {
              query.output.takeRight(dynamicPartitionColumns).map(SortOrder(_, Ascending)) ++ bucketSortOrders
            } else {
              bucketSortOrders
            }
            Sort(ordering, global = false, child = shuffle)
          } else {
            shuffle
          }
        case _ =>
          query
      }
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case AppendData(
        r@DataSourceV2Relation(table: OdpsTable, _, _, _, options), query, writeOptions, isByName, write, analyzedQuery)
          if !writeOptions.contains(WRITE_ODPS_TABLE_RESOLVED) =>
          var newOptions = writeOptions + Tuple2(WRITE_ODPS_TABLE_RESOLVED, "true")
          var partitionBuckets: Option[Int] = None
          var dynamicPartitionColumnsNum = 0
          if (table.partitionSchema.nonEmpty) {
            val (staticPartitionSpec, dynamicPartitionColumns) = getDynamicPartition(
              options.getOrDefault(WRITE_ODPS_STATIC_PARTITION, ""),
              table.partitionSchema)
            dynamicPartitionColumnsNum = dynamicPartitionColumns.size

            newOptions = newOptions +
              Tuple2(WRITE_ODPS_STATIC_PARTITION, staticPartitionSpec.toString(false, false))
            newOptions = newOptions +
              Tuple2(WRITE_ODPS_DYNAMIC_PARTITION, dynamicPartitionColumns.mkString(","))
          }

          val newQuery = insertRepartition(query, table, partitionBuckets, dynamicPartitionColumnsNum)
          AppendData(r, newQuery, newOptions, isByName, write, analyzedQuery)

        case OverwritePartitionsDynamic(
        r@DataSourceV2Relation(table: OdpsTable, _, _, _, options), query, writeOptions, isByName, write)
          if !writeOptions.contains(WRITE_ODPS_TABLE_RESOLVED) =>
          var newOptions = writeOptions + Tuple2(WRITE_ODPS_TABLE_RESOLVED, "true")
          var dynamicPartitionColumnsNum = 0
          if (table.partitionSchema.nonEmpty) {
            val (staticPartitionSpec, dynamicPartitionColumns) = getDynamicPartition(
              options.getOrDefault(WRITE_ODPS_STATIC_PARTITION, ""),
              table.partitionSchema)
            dynamicPartitionColumnsNum = dynamicPartitionColumns.size

            newOptions = newOptions +
              Tuple2(WRITE_ODPS_STATIC_PARTITION, staticPartitionSpec.toString(false, false))
            newOptions = newOptions +
              Tuple2(WRITE_ODPS_DYNAMIC_PARTITION, dynamicPartitionColumns.mkString(","))
          }
          val newQuery = insertRepartition(query, table, None, dynamicPartitionColumnsNum)
          OverwritePartitionsDynamic(r, newQuery, newOptions, isByName, write)
      }
    }
  }

  class OdpsStrategy(session: SparkSession) extends Strategy with SQLConfHelper {

    private def invalidateCache(
                                 r: ResolvedTable,
                                 recacheTable: Boolean = false)(): Option[StorageLevel] = {
      val v2Relation = DataSourceV2Relation.create(r.table, Some(r.catalog), Some(r.identifier))
      val cache = session.sharedState.cacheManager.lookupCachedData(v2Relation)
      session.sharedState.cacheManager.uncacheQuery(session, v2Relation, cascade = true)
      if (cache.isDefined) {
        val cacheLevel = cache.get.cachedRepresentation.cacheBuilder.storageLevel

        if (recacheTable) {
          val cacheName = cache.get.cachedRepresentation.cacheBuilder.tableName
          // recache with the same name and cache level.
          val ds = Dataset.ofRows(session, v2Relation)
          session.sharedState.cacheManager.cacheQuery(ds, cacheName, cacheLevel)
        }
        Some(cacheLevel)
      } else {
        None
      }
    }

    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case TruncateTable(
      r@ResolvedTable(_, _, table: OdpsTable, _)) =>
        TruncateTableExec(table, invalidateCache(r, recacheTable = true)) :: Nil

      case s@ShowColumnsCommand(table) =>
        ShowColumnsExec(s.output, table) :: Nil

      case OdpsHashRepartition(bucketAttributes, numBuckets, child) =>
        OdpsShuffleExchangeExec(
          OdpsHashPartitioning(bucketAttributes, numBuckets), planLater(child)) :: Nil

      case _ => Nil
    }
  }

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectResolutionRule(ResolveOdpsView)
    extensions.injectResolutionRule(new ResolveOdpsTable(_))
    extensions.injectOptimizerRule(new OptimizeWriteOdpsTable(_))
    extensions.injectPlannerStrategy(new OdpsStrategy(_))
  }
}
