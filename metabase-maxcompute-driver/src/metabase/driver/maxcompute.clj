(ns metabase.driver.maxcompute
    (:require
      [cheshire.core :as json]
      [clojure.core]
      [clojure.string :as str]
      [honey.sql :as hsql]
      [java-time.api :as t]
      [metabase.driver :as driver]
      [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
      [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
      [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
      [metabase.driver.sql.query-processor :as sql.qp]
      [metabase.lib.metadata :as lib.metadata]
      [metabase.query-processor.error-type :as qp.error-type]
      [metabase.query-processor.store :as qp.store]
      [metabase.query-processor.timezone :as qp.timezone]
      [metabase.query-processor.util.add-alias-info :as add]
      [metabase.util.date-2 :as u.date]
      [metabase.driver.sql.util.unprepare :as unprepare]
      [metabase.util.honey-sql-2 :as h2x]
      [metabase.legacy-mbql.util :as mbql.u]
      [clojure.string :as str]
      [honey.sql :as sql]
      [java-time.api :as t]
      [metabase.driver :as driver]
      [metabase.driver.common :as driver.common]
      [metabase.driver.sql :as driver.sql]
      [metabase.driver.sql.parameters.substitution :as sql.params.substitution]
      [metabase.driver.sql.query-processor :as sql.qp]
      [metabase.driver.sql.util :as sql.u]
      [metabase.driver.sql.util.unprepare :as unprepare]
      [metabase.legacy-mbql.util :as mbql.u]
      [metabase.lib.metadata :as lib.metadata]
      [metabase.lib.schema.metadata :as lib.schema.metadata]
      [metabase.models.setting :as setting]
      [metabase.query-processor.error-type :as qp.error-type]
      [metabase.query-processor.store :as qp.store]
      [metabase.query-processor.timezone :as qp.timezone]
      [metabase.query-processor.util.add-alias-info :as add]
      [metabase.util :as u]
      [metabase.util.date-2 :as u.date]
      [metabase.util.honey-sql-2 :as h2x]
      [metabase.util.i18n :refer [tru]]
      [metabase.util.log :as log]
      [metabase.util.malli :as mu]
      [metabase.models.secret :as secret])
    (:import
      (java.sql Connection ResultSet Time)
      (java.time LocalDate LocalDateTime LocalTime OffsetDateTime OffsetTime ZonedDateTime Instant)
      (metabase.driver.common.parameters FieldFilter)
      (java.util Date)
      (com.aliyun.odps Column Table Project Odps OdpsException)
      (com.aliyun.odps.jdbc OdpsConnection)
      (com.aliyun.odps.account AliyunAccount)))

(set! *warn-on-reflection* true)

(driver/register! :maxcompute, :parent :sql-jdbc)
(doseq [[feature supported?] {;; Does this database support following foreign key relationships while querying?
                              ;; Note that this is different from supporting primary key and foreign key constraints in the schema; see below.
                              :foreign-keys                           false

                              ;; Does this database track and enforce primary key and foreign key constraints in the schema?
                              ;; SQL query engines like Presto and Athena do not track these, though they can query across FKs.
                              ;; See :foreign-keys above.
                              :metadata/key-constraints               false

                              ;; Does this database support nested fields for any and every field except primary key (e.g. Mongo)?
                              :nested-fields                          false

                              ;; Does this database support nested fields but only for certain field types (e.g. Postgres and JSON / JSONB columns)?
                              :nested-field-columns                   true

                              ;; Does this driver support setting a timezone for the query?
                              :set-timezone                           false

                              ;; Does the driver support *basic* aggregations like `:count` and `:sum`? (Currently, everything besides standard
                              ;; deviation is considered \"basic\"; only GA doesn't support this).
                              :basic-aggregations                     true

                              ;; Does this driver support standard deviation and variance aggregations? Note that if variance is not supported
                              ;; directly, you can calculate it manually by taking the square of the standard deviation. See the MongoDB driver
                              ;; for example.
                              :standard-deviation-aggregations        false

                              ;; Does this driver support expressions (e.g. adding the values of 2 columns together)?
                              :expressions                            true

                              ;; Does this driver support parameter substitution in native queries, where parameter expressions are replaced
                              ;; with a single value? e.g.
                              ;;
                              ;;    SELECT * FROM table WHERE field = {{param}}
                              ;;    ->
                              ;;    SELECT * FROM table WHERE field = 1
                              :native-parameters                      true

                              ;; Does the driver support using expressions inside aggregations? e.g. something like \"sum(x) + count(y)\" or
                              ;; \"avg(x + y)\"
                              :expression-aggregations                true

                              ;; Does the driver support using a query as the `:source-query` of another MBQL query? Examples are CTEs or
                              ;; subselects in SQL queries.
                              :nested-queries                         false

                              ;; Does this driver support native template tag parameters of type `:card`, e.g. in a native query like
                              ;;
                              ;;    SELECT * FROM {{card}}
                              ;;
                              ;; do we support substituting `{{card}}` with another compiled (nested) query?
                              ;;
                              ;; By default, this is true for drivers that support `:native-parameters` and `:nested-queries`, but drivers can opt
                              ;; out if they do not support Card ID template tag parameters.
                              :native-parameter-card-reference        false

                              ;; Does the driver support persisting models
                              :persist-models                         false
                              ;; Is persisting enabled?
                              :persist-models-enabled                 false

                              ;; Does the driver support binning as specified by the `binning-strategy` clause?
                              :binning                                false

                              ;; Does this driver not let you specify whether or not our string search filter clauses (`:contains`,
                              ;; `:starts-with`, and `:ends-with`, collectively the equivalent of SQL `LIKE`) are case-senstive or not? This
                              ;; informs whether we should present you with the 'Case Sensitive' checkbox in the UI. At the time of this writing
                              ;; SQLite, SQLServer, and MySQL do not support this -- `LIKE` clauses are always case-insensitive.
                              ;;
                              ;; DEFAULTS TO TRUE.
                              :case-sensitivity-string-filter-options false

                              :left-join                              true
                              :right-join                             true
                              :inner-join                             true
                              :full-join                              false

                              :regex                                  false

                              ;; Does the driver support advanced math expressions such as log, power, ...
                              :advanced-math-expressions              false

                              ;; Does the driver support percentile calculations (including median)
                              :percentile-aggregations                false

                              ;; Does the driver support date extraction functions? (i.e year('1970/03/09'))
                              ;; DEFAULTS TO TRUE
                              :temporal-extract                       true

                              ;; Does the driver support doing math with datetime? (i.e Adding 1 year to a datetime column)
                              ;; DEFAULTS TO TRUE e.g. dateadd(datetime '2005-03-30 00:00:00', -1, 'mm');
                              :date-arithmetics                       true

                              ;; Does the driver support the :now function
                              :now                                    true

                              ;; Does the driver support converting timezone?
                              ;; DEFAULTS TO FALSE
                              :convert-timezone                       false

                              ;; Does the driver support :datetime-diff functions
                              :datetime-diff                          true

                              ;; Does the driver support experimental "writeback" actions like "delete this row" or "insert a new row" from 44+?
                              :actions                                false

                              ;; Does the driver support storing table privileges in the application database for the current user?
                              :table-privileges                       false

                              ;; Does the driver support uploading files
                              :uploads                                true

                              ;; Does the driver support schemas (aka namespaces) for tables
                              ;; DEFAULTS TO TRUE
                              :schemas                                true

                              ;; Does the driver support custom writeback actions. Drivers that support this must
                              ;; implement [[execute-write-query!]]
                              :actions/custom                         false

                              ;; Does changing the JVM timezone allow producing correct results? (See #27876 for details.)
                              :test/jvm-timezone-setting              false

                              ;; Does the driver support connection impersonation (i.e. overriding the role used for individual queries)?
                              :connection-impersonation               false

                              ;; Does the driver require specifying the default connection role for connection impersonation to work?
                              :connection-impersonation-requires-role false

                              ;; Does the driver require specifying a collection (table) for native queries? (mongo)
                              :native-requires-specified-collection   false

                              ;; Does the driver support column(s) support storing index info
                              :index-info                             false

                              ;; Does the driver support a faster `sync-fks` step by fetching all FK metadata in a single collection?
                              ;; if so, `metabase.driver/describe-fks` must be implemented instead of `metabase.driver/describe-table-fks`
                              :describe-fks                           false

                              ;; Does the driver support a faster `sync-fields` step by fetching all FK metadata in a single collection?
                              ;; if so, `metabase.driver/describe-fields` must be implemented instead of `metabase.driver/describe-table`
                              :describe-fields                        false

                              ;; Does the driver support automatically adding a primary key column to a table for uploads?
                              ;; If so, Metabase will add an auto-incrementing primary key column called `_mb_row_id` for any table created or
                              ;; updated with CSV uploads, and ignore any `_mb_row_id` column in the CSV file.
                              ;; DEFAULTS TO TRUE
                              :upload-with-auto-pk                    false

                              ;; Does the driver support fingerprint the fields. Default is true
                              :fingerprint                            false

                              ;; Does a connection to this driver correspond to a single database (false), or to multiple databases (true)?
                              ;; Default is false; ie. a single database. This is common for classic relational DBs and some cloud databases.
                              ;; Some have access to many databases from one connection; eg. Athena connects to an S3 bucket which might have
                              ;; many databases in it.
                              :connection/multiple-databases          false

                              ;; Does this driver support window functions like cumulative count and cumulative sum? (default: false)
                              :window-functions/cumulative            true

                              ;; Does this driver support the new `:offset` MBQL clause added in 50? (i.e. SQL `lag` and `lead` or equivalent
                              ;; functions)
                              :window-functions/offset                true
                              }]
       (defmethod driver/database-supports? [:maxcompute feature] [_driver _feature _db] supported?))

(def odps-instance (atom nil))
(defmethod driver/can-connect? :maxcompute
           [driver details]
           (let [{:keys [project endpoint ak sk namespace-schema]} details
                 account (AliyunAccount. ak (-> details (secret/db-details-prop->secret-map "sk")
                                                secret/value->string))
                 odps (Odps. account)]
                (.setEndpoint odps endpoint)
                (.setDefaultProject odps project)
                (.setCurrentSchema odps (if namespace-schema "default" nil))
                (try
                  (let [projects (.projects odps)]
                       (.exists projects project)
                       (reset! odps-instance odps)
                       true)
                  (catch OdpsException e
                    (println "driver/can-connect? - exception:" e)
                    false))))

;; this convert "a"."b"."c" to `a`.`b`.`c`, which is necessary for maxcompute
(defmethod sql.qp/quote-style :maxcompute [_] :mysql)

(defmethod sql-jdbc.conn/connection-details->spec :maxcompute
           [driver details-map]
           (let [{:keys [project endpoint ak sk timezone settings quotaName namespace-schema]} details-map
                 ;; 将 MaxCompute SQL 的默认 settings 放在这里
                 default-settings {"odps.sql.validate.orderby.limit" "false"
                                   "odps.sql.type.system.odps2"      "true"
                                   "odps.sql.allow.fullscan"         "true"
                                   "odps.namespace.schema"           "true"
                                   "odps.sql.bigquery.compatible"    "true"
                                   "odps.sql.timezone"               (or timezone "Asia/Shanghai")}
                 sk-value (-> details-map
                              (secret/db-details-prop->secret-map "sk")
                              secret/value->string)
                 settings-map (merge default-settings (try
                                                        (when settings
                                                              (json/parse-string settings true))
                                                        (catch Exception e
                                                          (println "Invalid settings JSON" settings)
                                                          {})))]
                (if (or (nil? endpoint) (nil? project) (nil? ak) (nil? sk-value))
                  (throw (IllegalArgumentException. "Missing required connection details"))
                  {:classname   "com.aliyun.odps.jdbc.OdpsDriver"
                   :subprotocol "odps"
                   :subname     (str endpoint "?project=" project
                                     "&enableOdpsLogger=true&charset=UTF-8&interactiveMode=true&enableLimit=false"
                                     (if (nil? quotaName) "" (str "&quotaName=" quotaName))
                                     "&settings=" (json/generate-string settings-map))
                   :user        ak
                   :password    sk-value})))

(defmethod driver/describe-database :maxcompute
           [driver database]
           (let [odps @odps-instance]
                (if-let [current-schema (.getCurrentSchema odps)]
                        ;; 新逻辑：存在 current-schema，遍历所有 schema 下的表
                        (let [default-project (.getDefaultProject odps)
                              schemas (iterator-seq (.iterator (.schemas odps)))
                              tables-metadata (mapcat (fn [schema]
                                                          (let [schema-name (.getName schema)
                                                                tables (iterator-seq (.iterator (.tables odps) default-project schema-name nil false))]
                                                               (map (fn [table]
                                                                        {:name                    (.getName table)
                                                                         :schema                  (or (.getSchemaName table) "default")
                                                                         :description             (.getComment table)
                                                                         :database_require_filter (.isPartitioned table)})
                                                                    tables)))
                                                      schemas)]
                             {:tables (set tables-metadata)})

                        ;; 旧逻辑：没有 current-schema，直接获取所有表
                        (let [tables-it (.iterator (.tables odps))
                              tables-metadata (loop [tables-metadata #{}]
                                                    (if (.hasNext tables-it)
                                                      (let [table (.next tables-it)
                                                            table-metadata {:name                    (.getName table)
                                                                            :schema                  (.getProject table)
                                                                            :description             (.getComment table)
                                                                            :database_require_filter (.isPartitioned table)}]
                                                           (recur (conj tables-metadata table-metadata)))
                                                      tables-metadata))]
                             {:tables tables-metadata}))))

(def ^:private database-type->base-type
  (sql-jdbc.sync/pattern-based-database-type->base-type
    [[#"BIGINT" :type/BigInteger]
     [#"TINYINT" :type/Integer]
     [#"SMALLINT" :type/Integer]
     [#"INT" :type/Integer]
     [#"CHAR" :type/Text]
     [#"STRING" :type/Text]
     [#"JSON" :type/Text]
     [#"VARCHAR" :type/Text]
     [#"BINARY" :type/*]
     [#"FLOAT" :type/Float]
     [#"DOUBLE" :type/Float]
     [#"DECIMAL" :type/Decimal]
     [#"BOOLEAN" :type/Boolean]
     [#"TIMESTAMP" :type/DateTimeWithTZ]
     [#"TIMESTAMP_NTZ" :type/DateTime]
     [#"DATETIME" :type/DateTimeWithTZ]
     [#"DATE" :type/Date]
     [#"ARRAY" :type/*]
     [#"MAP" :type/*]
     [#"STRUCT" :type/*]

     ]))
(defmethod sql-jdbc.sync/database-type->base-type :maxcompute
           [_ database-type]
           (database-type->base-type database-type))

;; maxcompute's JDBC driver is fussy and won't let you change connections to read-only after you create them. So skip that
;; step. maxcompute doesn't have a notion of session timezones so don't do that either. The only thing we're doing here from
;; the default impl is setting the transaction isolation level
(defmethod sql-jdbc.execute/do-with-connection-with-options :maxcompute
           [driver db-or-id-or-spec options f]
           (sql-jdbc.execute/do-with-resolved-connection
             driver
             db-or-id-or-spec
             options
             (fn [^Connection conn]
                 (f conn))))

;; maxcompute's JDBC driver is dumb and complains if you try to call `.setFetchDirection` on the Connection
(defmethod sql-jdbc.execute/prepared-statement :maxcompute
           [driver ^Connection conn ^String sql params]
           (let [stmt (.prepareStatement conn sql
                                         ResultSet/TYPE_FORWARD_ONLY
                                         ResultSet/CONCUR_READ_ONLY)]
                (try
                  (sql-jdbc.execute/set-parameters! driver stmt params)
                  stmt
                  (catch Throwable e
                    (.close stmt)
                    (throw e)))))
(defmethod sql.qp/->honeysql [:maxcompute :datetime-diff]
           [driver [_ x y unit]]
           (let [x (sql.qp/->honeysql driver x)
                 y (sql.qp/->honeysql driver y)]
                (:raw (if (nil? unit)
                        (str "DATEDIFF(" x ", " y ")")
                        (str "DATEDIFF(" x ", " y ", '-" unit "')")))))

(defn ^:private project-id-for-current-query
      []
      (when (qp.store/initialized?)
            (when-let [{:keys [details]} (lib.metadata/database (qp.store/metadata-provider))]
                      (:project details))))

(defn should-qualify-identifier? [_] true)                  ;


(defmethod sql.qp/->honeysql [:maxcompute ::h2x/identifier]
           [_driver identifier]
           (letfn [(prefix-components [[dataset-id table & more :as _components]]
                                      (cons (str (when-let [proj-id (project-id-for-current-query)]
                                                           (str proj-id \.))
                                                 dataset-id
                                                 \.
                                                 table)
                                            more))

                   (update-identifier-prefix-components [[_tag identifier-type components]]
                                                        (apply h2x/identifier identifier-type (prefix-components components)))]

                  (cond-> identifier
                          (should-qualify-identifier? identifier) update-identifier-prefix-components
                          true (vary-meta assoc ::do-not-qualify? true))))

(defn- valid-project-identifier?
       "Is String `s` a valid MaxCompute project identifier (a.k.a. project-id)? Identifiers are only allowed to contain
       letters, numbers, and underscores, cannot start with a number, and for project-id, can be at most 30 characters long."
       [s]
       (boolean (or (nil? s)
                    (and (string? s)
                         (re-matches #"^[a-zA-Z_0-9\.\-]{1,30}$" s)))))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                       Running Queries & Parsing Results                                        |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmulti parse-result-of-type
          "Parse the values that come back in results of a MaxCompute query based on their column type."
          {:added "0.41.0" :arglists '([column-type column-mode timezone-id v])}
          (fn [column-type _ _ _] column-type))

(defn- parse-value
       [column-mode v parse-fn]
         (parse-fn v))

(defmethod parse-result-of-type :default
           [_column-type column-mode _ v]
           (parse-value column-mode v identity))

(defmethod parse-result-of-type "STRING"
           [_a column-mode _b v]
           (parse-value column-mode v identity))

(defmethod parse-result-of-type "BOOLEAN"
           [_ column-mode _ v]
           (parse-value column-mode v #(Boolean/parseBoolean %)))

(defmethod parse-result-of-type "FLOAT"
           [_ column-mode _ v]
           (parse-value column-mode v #(Double/parseDouble %)))

(defmethod parse-result-of-type "INTEGER"
           [_ column-mode _ v]
           (parse-value column-mode v #(Long/parseLong %)))

(defmethod parse-result-of-type "NUMERIC"
           [_ column-mode _ v]
           (parse-value column-mode v bigdec))

(defmethod parse-result-of-type "BIGNUMERIC"
           [_column-type column-mode _timezone-id v]
           (parse-value column-mode v bigdec))

(defn- parse-timestamp-str [timezone-id s]
       ;; Timestamp strings either come back as ISO-8601 strings or Unix timestamps in µs, e.g. "1.3963104E9"
       (log/tracef "Parse timestamp string '%s' (default timezone ID = %s)" s timezone-id)
       (if-let [seconds (u/ignore-exceptions (Double/parseDouble s))]
               (t/zoned-date-time (t/instant (* seconds 1000)) (t/zone-id timezone-id))
               (u.date/parse s timezone-id)))

(defmethod parse-result-of-type "DATE"
           [_ column-mode _timezone-id v]
           (parse-value column-mode v u.date/parse))

(defmethod parse-result-of-type "DATETIME"
           [_ column-mode _timezone-id v]
           (parse-value column-mode v u.date/parse))

(defmethod parse-result-of-type "TIMESTAMP"
           [_ column-mode timezone-id v]
           (parse-value column-mode v (partial parse-timestamp-str timezone-id)))

(defmethod parse-result-of-type "TIME"
           [_ column-mode timezone-id v]
           (parse-value column-mode v (fn [v] (u.date/parse v timezone-id))))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                               SQL Driver Methods                                               |
;;; +----------------------------------------------------------------------------------------------------------------+

;; TODO -- all this [[temporal-type]] stuff below can be replaced with the more generalized
;; [[h2x/with-database-type-info]] stuff we've added. [[h2x/with-database-type-info]] was inspired by this MaxCompute code
;; but uses a new record type rather than attaching metadata to everything

(def ^:private temporal-type-hierarchy
  (-> (make-hierarchy)
      (derive :date :temporal-type)
      (derive :time :temporal-type)
      (derive :datetime :temporal-type)
      ;; timestamp = datetime with a timezone
      (derive :timestamp :temporal-type)
      (derive :timestamp_ntz :temporal-type)))

(defmulti ^:private temporal-type
          {:arglists '([x])}
          mbql.u/dispatch-by-clause-name-or-class
          :hierarchy #'temporal-type-hierarchy)

(defmethod temporal-type LocalDate      [_] :date)
;(defmethod temporal-type LocalTime      [_] :time)
;(defmethod temporal-type OffsetTime     [_] :time)
(defmethod temporal-type LocalDateTime  [_] :timestamp_ntz)
(defmethod temporal-type ZonedDateTime  [_] :datetime)
(defmethod temporal-type Instant  [_] :timestamp)

(defn- base-type->temporal-type [base-type]
       (condp #(isa? %2 %1) base-type
              :type/Date           :date
              ;:type/Time           :time
              :type/DateTimeWithTZ :timestamp
              :type/DateTimeWith   :timestamp_ntz
              :type/DateTime       :datetime
              nil))

(defn- database-type->temporal-type [database-type]
       (condp = (some-> database-type u/upper-case-en)
              "TIMESTAMP" :timestamp
              "DATETIME"  :datetime
              "DATE"      :date
              "TIMESTAMP_NTZ" :timestamp_ntz
              ;"TIME"      :time
              nil))

(defmethod temporal-type :metadata/column
           [{:keys [base-type effective-type database-type coercion-strategy]}]
           (or (when (isa? coercion-strategy :Coercion/UNIXTime->Temporal)
                     :timestamp)
               (base-type->temporal-type (or effective-type base-type))
               (database-type->temporal-type database-type)))

(defmethod temporal-type ::h2x/typed
           [form]
           (if (contains? (meta form) :maxcompute/temporal-type)
             (:maxcompute/temporal-type (meta form))
             (let [database-type (h2x/database-type form)]
                  (or (database-type->temporal-type database-type)
                      (temporal-type (h2x/unwrap-typed-honeysql-form form))))))

(defmethod temporal-type ::h2x/identifier
           [identifier]
           (:maxcompute/temporal-type (meta identifier)))

(defmethod temporal-type :absolute-datetime
           [[_ t _]]
           (temporal-type t))

(defmethod temporal-type :time
           [_]
           :time)

(defmethod temporal-type :field
           [[_ id-or-name {:keys [base-type effective-type temporal-unit]} :as clause]]
           (cond
             (contains? (meta clause) :maxcompute/temporal-type)
             (:maxcompute/temporal-type (meta clause))

             ;; date extraction operations result in integers, so the type of the expression shouldn't be a temporal type
             ;;
             ;; `:year` is both an extract unit and a truncate unit in terms of `u.date` capabilities, but in MBQL it should be a
             ;; truncation operation
             ((disj u.date/extract-units :year) temporal-unit)
             nil

             (integer? id-or-name)
             (temporal-type (lib.metadata/field (qp.store/metadata-provider) id-or-name))

             effective-type
             (base-type->temporal-type effective-type)

             base-type
             (base-type->temporal-type base-type)))

(defmethod temporal-type :default
           [x]
           (:maxcompute/temporal-type (meta x)))

(defn- with-temporal-type
       {:style/indent [:form]}
       [x new-type]
       (if (not (instance? clojure.lang.IObj x))
         x
         (vary-meta x assoc :maxcompute/temporal-type (keyword new-type))))

(defmulti ^:private ->temporal-type
          "Coerce `x` to target temporal type.

          `x` should be something that's already compiled to Honey SQL (i.e., call [[sql.qp/->honeysql]] on the arg before
          calling [[->temporal-type]]); and should return a Honey SQL form."
          {:arglists '([target-type x])}
          (fn [target-type x]
              [target-type (mbql.u/dispatch-by-clause-name-or-class x)])
          :hierarchy #'temporal-type-hierarchy)

(defn- throw-unsupported-conversion [from to]
       (throw (ex-info (tru "Cannot convert a {0} to a {1}" from to)
                       {:type qp.error-type/invalid-query})))

(defmethod ->temporal-type [:date LocalDate]           [_ t] t)
(defmethod ->temporal-type [:date LocalDateTime]       [_ t] (t/local-date t))
(defmethod ->temporal-type [:date Instant]             [_ t] (t/local-date t))
(defmethod ->temporal-type [:date ZonedDateTime]       [_ t] (t/local-date t))

(defmethod ->temporal-type [:datetime LocalDate]       [_ t] (t/local-date-time t (t/local-time 0)))
(defmethod ->temporal-type [:datetime LocalDateTime]   [_ t]  (t/local-date-time t))
(defmethod ->temporal-type [:datetime Instant]         [_ t] (t/local-date-time t))
(defmethod ->temporal-type [:datetime ZonedDateTime]   [_ t] (t))

;; Not sure whether we should be converting local dates/datetimes to ones with UTC timezone or with the report timezone?
(defmethod ->temporal-type [:timestamp LocalDate]      [_ t] (t/instant t (t/local-time 0) (t/zone-id "UTC")))
(defmethod ->temporal-type [:timestamp LocalDateTime]  [_ t] (t/instant t (t/zone-id "UTC")))
(defmethod ->temporal-type [:timestamp Instant]        [_ t] (t))
(defmethod ->temporal-type [:timestamp ZonedDateTime]  [_ t] (t/instant t ))


(defmethod ->temporal-type [:timestamp_ntz LocalDate]       [_ t] (t/local-date-time t (t/local-time 0)))
(defmethod ->temporal-type [:timestamp_ntz LocalDateTime]   [_ t]  (t/local-date-time t))
(defmethod ->temporal-type [:timestamp_ntz Instant]         [_ t] (t/local-date-time t))
(defmethod ->temporal-type [:timestamp_ntz ZonedDateTime]   [_ t] (t))

(defmethod ->temporal-type :default
           [target-type x]
           (when (some? x)
                 (let [current-type (temporal-type x)]
                      (cond
                        (= current-type target-type)
                        x

                        (contains? #{:date :timestamp_ntz :datetime :timestamp} target-type)
                        (do
                          (log/tracef "Coercing %s (temporal type = %s) to %s"
                                      (binding [*print-meta* true] (pr-str x))
                                      (pr-str (temporal-type x))
                                      target-type)
                          (let [expr (if-let [report-zone (when (or (= current-type :timestamp)
                                                                    (= target-type :timestamp))
                                                                (qp.timezone/requested-timezone-id))]
                                             [target-type x (h2x/literal report-zone)]
                                             [target-type x])]
                               (with-temporal-type expr target-type)))

                        :else
                        x))))

(defmethod ->temporal-type [:temporal-type :absolute-datetime]
           [target-type [_ t unit]]
           [:absolute-datetime (->temporal-type target-type t) unit])

(def ^:private temporal-type->supported-units
  {:timestamp #{:microsecond :millisecond :second :minute :hour :day}
   :timestamp_ntz  #{:microsecond :millisecond :second :minute :hour :day}
   :datetime  #{:microsecond :millisecond :second :minute :hour :day :week :month :quarter :year}
   :date      #{:day :week :month :quarter :year}
   :time      #{:microsecond :millisecond :second :minute :hour}})

(defmethod ->temporal-type [:temporal-type :relative-datetime]
           [target-type [_ _ unit :as clause]]
           {:post [(= target-type (temporal-type %))]}
           (with-temporal-type
             ;; check and see whether we need to do a conversion. If so, use the parent method which will just wrap this in a
             ;; cast statement.
             (if ((temporal-type->supported-units target-type) unit)
               clause
               ((get-method ->temporal-type :default) target-type clause))
             target-type))

(defn- format-trunc
       [_tag [expr unit report-timezone :as _args]]
       (let [t               (or (temporal-type expr) :datetime)
             f               (case t
                                   :date      :datetrunc
                                   :time      :datetrunc
                                   :datetime  :datetrunc
                                   :timestamp :datetrunc
                                   :timestamp_ntz :datetrunc)
             unit-expr       (h2x/literal (name unit))
             expr            (if (and report-timezone
                                      (= f :datetrunc))
                               [f expr unit-expr (h2x/literal report-timezone)]
                               [f expr unit-expr])]
            (sql/format-expr expr {:nested true})))

(sql/register-fn! ::trunc #'format-trunc)

(defmethod temporal-type ::trunc
           [[_trunc-form expr _unit _report-timezone :as form]]
           (or (:maxcompute/temporal-type (meta form))
               (temporal-type expr)))

(defmethod ->temporal-type [:temporal-type ::trunc]
           [target-type [_trunc-form expr unit report-timezone]]
           [::trunc (->temporal-type target-type expr) unit report-timezone])

(defn- trunc
       "Generate a SQL call an appropriate truncation function, depending on the temporal type of `expr`."
       [unit expr]
       [::trunc expr unit (qp.timezone/requested-timezone-id)])

(def ^:private valid-date-extract-units
  #{:dayofweek :day :dayofyear :week :isoweek :month :quarter :year :isoyear})

(def ^:private valid-time-extract-units
  #{:microsecond :millisecond :second :minute :hour})

(defn- format-extract
       [_tag [unit expr timezone]]
       (let [[expr-sql & expr-args] (sql/format-expr expr {:nested true})
             [zone-sql & zone-args] (when timezone
                                          (sql/format-expr (h2x/literal timezone) {:nested true}))]
            (into [((clojure.core/format "EXTRACT(%s FROM %s)" (name unit) expr-sql))]
                  cat
                  [expr-args
                   zone-args])))

(sql/register-fn! ::extract #'format-extract)

(defn- extract*
       ([unit expr]
        (extract* unit expr nil))
       ([unit expr timezone]
        [::extract unit expr timezone]))

(defn- extract [unit expr]
       (condp = (temporal-type expr)
              :time
              (do
                (assert (valid-time-extract-units unit)
                        (tru "Cannot extract {0} from a TIME field" unit))
                (recur unit (with-temporal-type [:timestamp [:datetime [:inline "1970-01-01"] expr]]
                                                :timestamp)))

              ;; timestamp and date both support extract()
              :date
              (do
                (assert (valid-date-extract-units unit)
                        (tru "Cannot extract {0} from a DATE field" unit))
                (with-temporal-type (extract* unit expr) nil))

              :timestamp
              (do
                (assert (or (valid-date-extract-units unit)
                            (valid-time-extract-units unit))
                        (tru "Cannot extract {0} from a DATETIME or TIMESTAMP" unit))
                (with-temporal-type (extract* unit expr (qp.timezone/requested-timezone-id)) nil))

              ;; for datetimes or anything without a known temporal type, cast to timestamp and go from there
              (recur unit (->temporal-type :timestamp expr))))

(defmethod sql.qp/date [:maxcompute :second-of-minute] [_ _ expr] (extract :second    expr))
(defmethod sql.qp/date [:maxcompute :minute]           [_ _ expr] (trunc   :minute    expr))
(defmethod sql.qp/date [:maxcompute :minute-of-hour]   [_ _ expr] (extract :minute    expr))
(defmethod sql.qp/date [:maxcompute :hour]             [_ _ expr] (trunc   :hour      expr))
(defmethod sql.qp/date [:maxcompute :hour-of-day]      [_ _ expr] (extract :hour      expr))
(defmethod sql.qp/date [:maxcompute :day]              [_ _ expr] (trunc   :day       expr))
(defmethod sql.qp/date [:maxcompute :day-of-month]     [_ _ expr] (extract :day       expr))
(defmethod sql.qp/date [:maxcompute :day-of-year]      [_ _ expr] (extract :dayofyear expr))
(defmethod sql.qp/date [:maxcompute :month]            [_ _ expr] (trunc   :month     expr))
(defmethod sql.qp/date [:maxcompute :month-of-year]    [_ _ expr] (extract :month     expr))
(defmethod sql.qp/date [:maxcompute :quarter]          [_ _ expr] (trunc   :quarter   expr))
(defmethod sql.qp/date [:maxcompute :quarter-of-year]  [_ _ expr] (extract :quarter   expr))
(defmethod sql.qp/date [:maxcompute :year]             [_ _ expr] (trunc   :year      expr))
(defmethod sql.qp/date [:maxcompute :year-of-era]      [_ _ expr] (extract :year      expr))

(defn- format-mod
       "MaxCompute mod is a function like mod(x, y) rather than an operator like x mod y."
       [_tag [x y :as _args]]
       (let [[x-sql & x-args] (sql/format-expr x {:nested true})
             [y-sql & y-args] (sql/format-expr y {:nested true})]
            (into [(format "mod(%s, %s)" x-sql y-sql)]
                  cat
                  [x-args y-args])))

(sql/register-fn! ::mod #'format-mod)

(defmethod sql.qp/date [:maxcompute :day-of-week]
           [driver _ expr]
           (sql.qp/adjust-day-of-week
             driver
             (extract :dayofweek expr)
             (driver.common/start-of-week-offset driver)
             (fn [x y]
                 [::mod x y])))

(defmethod sql.qp/date [:maxcompute :week]
           [_driver _unit expr]
           (trunc (keyword (format "week(%s)" (name (setting/get-value-of-type :keyword :start-of-week)))) expr))

;; TODO: maxcompute supports week(weekday), maybe we don't have to do the complicated math for maxcompute?
(defmethod sql.qp/date [:maxcompute :week-of-year-iso]
           [_driver _unit expr]
           (extract :isoweek expr))

(doseq [[unix-timestamp-type maxcompute-fn] {:seconds      :timestamp_seconds
                                           :milliseconds :timestamp_millis
                                           :microseconds :timestamp_micros}]
       (defmethod sql.qp/unix-timestamp->honeysql [:maxcompute unix-timestamp-type]
                  [_driver _unix-timestamp-type expr]
                  (-> [maxcompute-fn expr]
                      (with-temporal-type :timestamp)
                      (h2x/with-database-type-info "timestamp")
                      (with-temporal-type :timestamp))))

(defmethod sql.qp/->honeysql [:maxcompute :convert-timezone]
           [driver [_ arg target-timezone source-timezone]]
           (let [datetime     (fn [x target-timezone]
                                  [:datetime x target-timezone])
                 hsql-form    (sql.qp/->honeysql driver arg)
                 timestamptz? (h2x/is-of-type? hsql-form "timestamp")]
                (sql.u/validate-convert-timezone-args timestamptz? target-timezone source-timezone)
                (-> (if timestamptz?
                      hsql-form
                      [:timestamp hsql-form (or source-timezone (qp.timezone/results-timezone-id))])
                    (datetime target-timezone)
                    (with-temporal-type :datetime))))

(defmethod sql.qp/->float :maxcompute
           [_ value]
           (h2x/cast :float64 value))

(defmethod sql.qp/->honeysql [:maxcompute :regex-match-first]
           [driver [_ arg pattern]]
           [:regexp_extract (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern)])

(defn- percentile->quantile
       [x]
       (loop [x     (double x)
              power (int 0)]
             (if (zero? (- x (Math/floor x)))
               [(Math/round x) (Math/round (Math/pow 10 power))]
               (recur (* 10 x) (inc power)))))

(defn- format-approx-quantiles
       [_tag [expr offset quantiles :as _args]]
       (let [[expr-sql & expr-args]           (sql/format-expr expr {:nested true})
             [offset-sql & offset-args]       (sql/format-expr offset {:nested true})
             [quantiles-sql & quantiles-args] (sql/format-expr quantiles {:nested true})]
            (into [(format "APPROX_QUANTILES(%s, %s)[OFFSET(%s)]" expr-sql quantiles-sql offset-sql)]
                  cat
                  [expr-args quantiles-args offset-args])))

(sql/register-fn! ::approx-quantiles #'format-approx-quantiles)

(defn- approx-quantiles
       "HoneySQL form for the APPROX_QUANTILES invocation. The [OFFSET(...)] part after the function call is odd and
       needs special treatment."
       [expr offset quantiles]
       (let [offset    (if (number? offset)
                         [:inline offset]
                         offset)
             quantiles (if (number? quantiles)
                         [:inline quantiles]
                         quantiles)]
            [::approx-quantiles expr offset quantiles]))

(defmethod sql.qp/->honeysql [:maxcompute :percentile]
           [driver [_ expr p]]
           (let [[offset quantiles] (percentile->quantile p)]
                (approx-quantiles (sql.qp/->honeysql driver expr) offset quantiles)))

(defmethod sql.qp/->honeysql [:maxcompute :median]
           [driver [_ arg]]
           (sql.qp/->honeysql driver [:percentile arg 0.5]))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                Query Processor                                                 |
;;; +----------------------------------------------------------------------------------------------------------------+

;; this is a little hacky, I'm 99% sure we could just have the [[sql.qp/->honeysql]] method for `:field` swap out the
;; `::add/source-table` to a `[project.dataset table]` pair but this will have to do for now.
(def ^:private ^:dynamic *field-is-from-join-or-source-query?* false)

(defn- should-qualify-identifier?
       "Should we qualify an [[h2x/identifier]] with the dataset name?

       Table & Field identifiers (usually) need to be qualified with the current dataset name; this needs to be part of the
       table e.g.

         `table`.`field` -> `dataset.table`.`field`"
       [[_tag identifier-type components, :as identifier]]
       (cond
         (::do-not-qualify? (meta identifier))
         false

         ;; If we're currently using a Table alias, don't qualify the alias with the dataset name
         *field-is-from-join-or-source-query?*
         false

         ;; otherwise always qualify Table identifiers
         (= identifier-type :table)
         true

         ;; Only qualify Field identifiers that are qualified by a Table. (e.g. don't qualify stuff inside `CREATE TABLE`
         ;; DDL statements)
         (and (= identifier-type :field)
              (>= (count components) 2))
         true))

(defmethod sql.qp/cast-temporal-string [:maxcompute :Coercion/YYYYMMDDHHMMSSString->Temporal]
           [_driver _coercion-strategy expr]
           [:parse_datetime (h2x/literal "%Y%m%d%H%M%S") expr])

(defmethod sql.qp/->honeysql [:maxcompute ::h2x/identifier]
           [_driver identifier]
           (letfn [(prefix-components [[dataset-id table & more :as _components]]
                                      (cons (str (when-let [proj-id (project-id-for-current-query)]
                                                           (str proj-id \.))
                                                 dataset-id
                                                 \.
                                                 table)
                                            more))
                   (update-identifier-prefix-components [[_tag identifier-type components]]
                                                        (apply h2x/identifier identifier-type (prefix-components components)))]
                  (cond-> identifier
                          (should-qualify-identifier? identifier) update-identifier-prefix-components
                          true                                    (vary-meta assoc ::do-not-qualify? true))))

(defmethod sql.qp/->honeysql [:maxcompute :field]
           [driver [_field _id-or-name {::add/keys [source-table], :as _opts} :as field-clause]]
           (let [parent-method (get-method sql.qp/->honeysql [:sql :field])]
                ;; if the Field is from a join or source table, record this fact so that we know never to qualify it with the
                ;; project ID no matter what
                (binding [*field-is-from-join-or-source-query?* (not (integer? source-table))]
                         ;; attach temporal type info to the field clause, this will get attached to the resulting [[h2x/identifier]] by
                         ;; SQL QP parent method, and we can access that inside other things like [[sql.qp/date]] implementations which it
                         ;; may call in turn.
                         (let [field-clause (with-temporal-type field-clause (temporal-type field-clause))
                               result       (parent-method driver field-clause)]
                              (cond-> result
                                      (not (temporal-type result)) (with-temporal-type (temporal-type field-clause)))))))

(defmethod sql.qp/->honeysql [:maxcompute :relative-datetime]
           [driver clause]
           ;; wrap the parent method, converting the result if `clause` itself is typed
           (let [t (temporal-type clause)]
                (cond->> ((get-method sql.qp/->honeysql [:sql :relative-datetime]) driver clause)
                         t (->temporal-type t))))

(defn- datetime-diff-check-args
       "Validates the types of the datetime args to a `datetime-diff` clause. This is exactly the same
       as [[sql.qp/datetime-diff-check-args]] except it uses [[temporal-type]]` to get the type of each arg,
       not [[h2x/database-type]], which is needed for maxcompute."
       [x y]
       (doseq [arg [x y]
               :let [db-type (some-> (temporal-type arg) name)]
               :when (and db-type (not (re-find #"^(?i)(timestamp|date)" db-type)))]
              (throw (ex-info (tru "datetimeDiff only allows datetime, timestamp, or date types. Found {0}"
                                   (pr-str db-type))
                              {:found db-type
                               :type  qp.error-type/invalid-query}))))

(defmethod sql.qp/->honeysql [:maxcompute :datetime-diff]
           [driver [_ x y unit]]
           (let [x (sql.qp/->honeysql driver x)
                 y (sql.qp/->honeysql driver y)]
                (datetime-diff-check-args x y)
                (sql.qp/datetime-diff driver unit x y)))

(defn- timestamp-diff [unit x y]
       [:timestamp_diff
        (->temporal-type :timestamp y)
        (->temporal-type :timestamp x)
        [:raw (name unit)]])

(defmethod sql.qp/datetime-diff [:maxcompute :year]
           [driver _unit x y]
           (h2x// (sql.qp/datetime-diff driver :month x y) 12))

(defmethod sql.qp/datetime-diff [:maxcompute :quarter]
           [driver _unit x y]
           (h2x// (sql.qp/datetime-diff driver :month x y) 3))

(defmethod sql.qp/datetime-diff [:maxcompute :month]
           [_driver _unit x y]
           ;; Only maxcompute's `datetime_diff` supports months. We need to convert args to datetime to use it.
           ;; Also `<` and `>` comparisons can only be made on the same type.
           (let [x' (->temporal-type :datetime x)
                 y' (->temporal-type :datetime y)]
                (h2x/+ [:datetime_diff y' x' [:raw "month"]]
                       ;; datetime_diff counts month boundaries not whole months, so we need to adjust
                       ;; if x<y but x>y in the month calendar then subtract one month
                       ;; if x>y but x<y in the month calendar then add one month
                       [:case
                        [:and [:< x' y'] [:> (extract :day x) (extract :day y)]]
                        -1
                        [:and [:> x' y'] [:< (extract :day x) (extract :day y)]]
                        1
                        :else 0])))

(defmethod sql.qp/datetime-diff [:maxcompute :week]
           [driver _unit x y]
           (h2x// (sql.qp/datetime-diff driver :day x y) 7))

(defmethod sql.qp/datetime-diff [:maxcompute :day]
           [_driver _unit x y]
           (timestamp-diff :day (trunc :day x) (trunc :day y)))

(defmethod sql.qp/datetime-diff [:maxcompute :hour] [_driver _unit x y] (timestamp-diff :hour x y))
(defmethod sql.qp/datetime-diff [:maxcompute :minute] [_driver _unit x y] (timestamp-diff :minute x y))
(defmethod sql.qp/datetime-diff [:maxcompute :second] [_driver _unit x y] (timestamp-diff :second x y))

(defmethod driver/escape-alias :maxcompute
           [driver s]
           ;; Convert field alias `s` to a valid MaxCompute field identifier. From the dox: Fields must contain only letters,
           ;; numbers, and underscores, start with a letter or underscore, and be at most 128 characters long.
           (let [s (-> (str/trim s)
                       u/remove-diacritical-marks
                       (str/replace #"[^\w\d_]" "_")
                       (str/replace #"(^\d)" "_$1"))]
                ((get-method driver/escape-alias :sql) driver s)))

(defmethod unprepare/unprepare-value [:maxcompute String]
           [_ s]
           ;; escape single-quotes like Cam's String -> Cam\'s String
           (str \' (str/replace s "'" "\\\\'") \'))

(defmethod unprepare/unprepare-value [:maxcompute LocalTime]
           [_ t]
           (format "datetime\"%s\"" (u.date/format-sql t)))

(defmethod unprepare/unprepare-value [:maxcompute LocalDate]
           [_ t]
           (format "date\"%s\"" (u.date/format-sql t)))

(defmethod unprepare/unprepare-value [:maxcompute LocalDateTime]
           [_ t]
           (format "timestamp\"%s\"" (u.date/format-sql t)))

(defmethod unprepare/unprepare-value [:maxcompute Instant]
           [_ t]
           (format "timestamp\"%s\"" (u.date/format-sql t)))

(defmethod unprepare/unprepare-value [:maxcompute OffsetTime]
           [_ t]
           ;; convert to a LocalTime in UTC
           (let [local-time (t/local-time (t/with-offset-same-instant t (t/zone-offset 0)))]
                (format "datetime\"%s\"" (u.date/format-sql local-time))))

(defmethod unprepare/unprepare-value [:maxcompute OffsetDateTime]
           [_ t]
           (format "datetime\"%s\"" (u.date/format-sql t)))

(defmethod unprepare/unprepare-value [:maxcompute ZonedDateTime]
           [_ t]
           (format "datetime\"%s %s\"" (u.date/format-sql (t/local-date-time t)) (.getId (t/zone-id t))))

(def ^:private ^:dynamic *compiling-cumulative-aggregation* false)

(defmethod sql.qp/->honeysql [:maxcompute :cum-count]
           [driver expr]
           (binding [*compiling-cumulative-aggregation* true]
                    ((get-method sql.qp/->honeysql [:sql :cum-count]) driver expr)))

(defmethod sql.qp/->honeysql [:maxcompute :cum-sum]
           [driver expr]
           (binding [*compiling-cumulative-aggregation* true]
                    ((get-method sql.qp/->honeysql [:sql :cum-sum]) driver expr)))

(defmethod sql.qp/apply-top-level-clause [:maxcompute :breakout]
           [driver top-level-clause honeysql-form query]
           (if *compiling-cumulative-aggregation*
             ((get-method sql.qp/apply-top-level-clause [:sql :breakout]) driver top-level-clause honeysql-form query)
             ;; If stuff in `:fields` still needs to be qualified like `dataset.table.field`, just the stuff in `:group-by` should
             ;; not. So we'll actually call the parent method twice, once with the fields as is (i.e., qualifiable) and once with
             ;; them removed. Then we'll splice the unqualified `:group-by` in
             (let [parent-method (partial (get-method sql.qp/apply-top-level-clause [:sql :breakout])
                                          driver top-level-clause honeysql-form)
                   qualified     (parent-method query)
                   unqualified   (parent-method (update query :breakout sql.qp/rewrite-fields-to-force-using-column-aliases))]
                  (merge qualified
                         (select-keys unqualified #{:group-by})))))

(defmethod sql.qp/->honeysql [:maxcompute :asc]
           [driver clause]
           ((get-method sql.qp/->honeysql [:sql :asc])
            driver
            (sql.qp/rewrite-fields-to-force-using-column-aliases clause)))

(defmethod sql.qp/->honeysql [:maxcompute :desc]
           [driver clause]
           ((get-method sql.qp/->honeysql [:sql :desc])
            driver
            (sql.qp/rewrite-fields-to-force-using-column-aliases clause)))

(defmethod temporal-type ::sql.qp/compiled
           [[_compiled x, :as form]]
           (or (:maxcompute/temporal-type (meta form))
               (temporal-type x)))

(defmethod ->temporal-type ::sql.qp/compiled
           [target-type form]
           (-> (sql.qp/compiled (->temporal-type target-type form))
               (vary-meta assoc :maxcompute/temporal-type target-type)))

;(defn- reconcile-temporal-types
;       "Make sure the temporal types of fields and values in filter clauses line up."
;       [[tag & args :as clause]]
;       (if (#{:and :or :not} tag)
;         (into [tag] (map reconcile-temporal-types) args)
;         (println "Reconciling temporal types args:" args)
;         (if-let [target-type (some temporal-type args)]
;                 (do
;                   (log/tracef "Coercing args in %s to temporal type %s" (binding [*print-meta* true] (pr-str clause)) target-type)
;                   (u/prog1 (into [tag]
;                                  (map (partial ->temporal-type target-type))
;                                  args)
;                            (when (or (not= clause <>)
;                                      (not= (meta clause) (meta <>)))
;                                  (log/tracef "Coerced -> %s" (binding [*print-meta* true] (pr-str <>))))))
;                 clause)))

(defn- reconcile-temporal-types
       "Make sure the temporal types of fields and values in filter clauses line up."
       [[tag & args :as clause]]
       (if (#{:and :or :not} tag)
         ;; 如果是逻辑操作符，递归处理每个子句
         (into [tag] (map reconcile-temporal-types) args)
         ;; 否则处理非逻辑操作符的情况
         (do
           (if-let [target-type (some temporal-type args)]
                   (do
                     (log/tracef "Coercing args in %s to temporal type %s" (binding [*print-meta* true] (pr-str clause)) target-type)
                     (u/prog1
                       (into [tag] (map (partial ->temporal-type target-type)) args)
                       (let [new-clause <>]
                            ;(println "Original clause:" (pr-str clause)) ; 打印原始子句
                            ;(println "Transformed clause:" (pr-str new-clause)) ; 打印转换后的子句
                            (when (or (not= clause new-clause)
                                      (not= (meta clause) (meta new-clause)))
                                  (log/tracef "Coerced -> %s" (binding [*print-meta* true] (pr-str new-clause)))))))
                   clause))))

(doseq [filter-type [:between := :!= :> :>= :< :<=]]
       (defmethod sql.qp/->honeysql [:maxcompute filter-type]
                  [driver clause]
                  (reconcile-temporal-types
                    ((get-method sql.qp/->honeysql [:sql filter-type])
                     driver
                     clause))))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                Other Driver / SQLDriver Method Implementations                                 |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- interval [amount unit]
       ;; todo: can maxcompute have an expression here or just a numeric literal?
       [:raw (format "INTERVAL %d %s" (int amount) (name unit))])

;; We can coerce the HoneySQL form this wraps to whatever we want and generate the appropriate SQL.
;; Thus for something like filtering against a relative datetime
;;
;; [:time-interval <datetime field> -1 :day]
;;
;;
(def ^:private temporal-type->arithmetic-function
  {:timestamp :dateadd
   :datetime  :dateadd
   :date      :dateadd
   :time      :dateadd})

(defn- format-add-interval
       [_tag [expr amount unit :as _args]]
       (let [t      (temporal-type expr)
             add-fn (temporal-type->arithmetic-function t)]
            (sql/format-expr [add-fn expr amount (name unit)] {:nested true})))

(sql/register-fn! ::add-interval #'format-add-interval)

(defn- add-interval-form
       "Some units aren't supported for some target types (e.g. you cannot add a year to a timestamp for whatever dumb
       reason), so this may return a `:datetime` expression instead."
       [expr amount unit]
       (let [t      (temporal-type expr)
             add-fn (temporal-type->arithmetic-function t)
             expr   (if (or (not add-fn)
                            (and (not (contains? (temporal-type->supported-units t) unit))
                                 (contains? (temporal-type->supported-units :datetime) unit)))
                      (->temporal-type :datetime expr)
                      expr)]
            [::add-interval expr amount unit]))

(defmethod temporal-type ::add-interval
           [[_add-interval expr _amount _unit]]
           (temporal-type expr))

(defmethod ->temporal-type [:temporal-type ::add-interval]
           [target-type [_add-interval expr amount unit :as original-form]]
           (let [current-type (temporal-type expr)]
                (when (#{[:date :time] [:time :date]} [current-type target-type])
                      (throw (ex-info (tru "It doesn''t make sense to convert between DATEs and TIMEs!")
                                      {:type qp.error-type/invalid-query}))))
           ;; [[add-interval-form]] might return something of a different type than `target-type`, depending on unit... in that
           ;; case, just wrap the original `::add-interval` clause in a `cast` expression instead.
           (let [new-form (add-interval-form (->temporal-type target-type expr) amount unit)]
                (if (= (temporal-type new-form) target-type)
                  new-form
                  ((get-method ->temporal-type :default) target-type original-form))))

(defmethod sql.qp/add-interval-honeysql-form :maxcompute
           [_ hsql-form amount unit]
           ;; `timestamp_add()` doesn't support month/quarter/year, so cast it to `datetime` so we can use `datetime_add()`
           ;; instead in those cases.
           (let [hsql-form (cond->> hsql-form
                                    (and (= (temporal-type hsql-form) :timestamp)
                                         (not (contains? (temporal-type->supported-units :timestamp) unit)))
                                    (h2x/cast :datetime))]
                (add-interval-form hsql-form amount unit)))

(defmethod driver/mbql->native :maxcompute
           [driver outer-query]
           (let [parent-method (get-method driver/mbql->native :sql)
                 compiled      (parent-method driver outer-query)]
                (assoc compiled
                       :table-name (or (when-let [source-table-id (get-in outer-query [:query :source-table])]
                                                 (:name (lib.metadata/table (qp.store/metadata-provider) source-table-id)))
                                       sql.qp/source-query-alias)
                       :mbql?      true)))

(defn- format-current-moment
       [_tag [target-type report-timezone :as _args]]
       (let [f           (case (or target-type :timestamp)
                               :time      :current_timestamp
                               :date      :current_timestamp
                               :datetime  :current_timestamp
                               :timestamp_ntz :current_timestamp
                               :timestamp :current_timestamp)
             is-timestamp (when (not= target-type :timestamp) true)]
            (sql/format-expr
              (if is-timestamp
                [:cast [f] target-type]
                [:cast [f] target-type])
              {:nested true})))

(sql/register-fn! ::current-moment #'format-current-moment)

(defmethod temporal-type ::current-moment
           [[_current-moment target-type _report-timezone]]
           target-type)

(defmethod ->temporal-type [:temporal-type ::current-moment]
           [new-target-type [_current-moment _old-target-type report-timezone]]
           [::current-moment new-target-type report-timezone])

(defmethod sql.qp/current-datetime-honeysql-form :maxcompute
           [_driver]
           [::current-moment nil (qp.timezone/requested-timezone-id)])

(defmethod sql.qp/->honeysql [:maxcompute :now]
           [driver _clause]
           (->> (sql.qp/current-datetime-honeysql-form driver)
                (->temporal-type :timestamp)))

;; In MaxCompute, log syntax is `log(x, base)`
(defmethod sql.qp/->honeysql [:maxcompute :log]
           [driver [_ field]]
           [:log (sql.qp/->honeysql driver field) [:inline 10]])

(defmethod sql.qp/quote-style :maxcompute
           [_driver]
           :mysql)

;; convert OffsetDateTime to an ZonedDateTime in UTC since MaxCompute doesn't handle ZonedDateTime as we'd like
;(defmethod driver.sql/->prepared-substitution [:maxcompute OffsetDateTime]
;           [driver t]
;           (driver.sql/->prepared-substitution driver (t/zoned-date-time result (t/zone-id "UTC"))))

(mu/defmethod sql.params.substitution/->replacement-snippet-info [:maxcompute FieldFilter]
              [driver                            :- :keyword
               {:keys [field], :as field-filter} :- [:map
                                                     [:field ::lib.schema.metadata/column]]]
              (let [field-temporal-type (temporal-type field)
                    parent-method       (get-method sql.params.substitution/->replacement-snippet-info [:sql FieldFilter])
                    result              (parent-method driver field-filter)]
                   (cond-> result
                           field-temporal-type (update :prepared-statement-args (fn [args]
                                                                                    (let [request-time-zone-id (qp.timezone/requested-timezone-id)]
                                                                                         (map (fn [arg]
                                                                                                  (if (instance? java.time.temporal.Temporal arg)
                                                                                                    ;; Since we add the zone as part of the
                                                                                                    ;; LHS of the filter, we need to add the zone to
                                                                                                    ;; the RHS as well.
                                                                                                    (let [result (->temporal-type field-temporal-type arg)]
                                                                                                         (cond
                                                                                                           (or (not request-time-zone-id)
                                                                                                               (not= :type/DateTimeWithLocalTZ (:base-type field)))
                                                                                                           result

                                                                                                           (instance? java.time.ZonedDateTime result)
                                                                                                           (t/with-zone-same-instant result request-time-zone-id)

                                                                                                           (instance? java.time.OffsetDateTime result)
                                                                                                           (t/with-zone-same-instant (t/zoned-date-time result) request-time-zone-id)))
                                                                                                    arg))
                                                                                              args)))))))

(defmethod sql.qp/cast-temporal-string [:maxcompute :Coercion/ISO8601->DateTime]
           [_driver _semantic_type expr]
           (h2x/->datetime expr))

(defmethod sql.qp/cast-temporal-string [:maxcompute :Coercion/ISO8601->Date]
           [_driver _semantic_type expr]
           (h2x/->date expr))

(defmethod sql.qp/cast-temporal-string [:maxcompute :Coercion/ISO8601->Time]
           [_driver _semantic_type expr]
           (h2x/->time expr))

