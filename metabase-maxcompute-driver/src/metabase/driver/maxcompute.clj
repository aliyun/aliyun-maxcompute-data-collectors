(ns metabase.driver.maxcompute
  (:require
   [cheshire.core :as json]
   [clojure.string :as str]
   [honey.sql :as sql]
   [java-time.api :as t]
   [metabase.driver :as driver]
   [metabase.driver.common :as driver.common]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
   [metabase.driver.sql.parameters.substitution :as sql.params.substitution]
   [metabase.driver.sql.query-processor :as sql.qp]
   [metabase.driver.sql.util :as sql.u]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.lib.schema.metadata :as lib.schema.metadata]
   [metabase.query-processor.error-type :as qp.error-type]
   [metabase.query-processor.store :as qp.store]
   [metabase.query-processor.timezone :as qp.timezone]
   [metabase.query-processor.util.add-alias-info :as add]
   [metabase.util :as u]
   [metabase.util.date-2 :as u.date]
   [metabase.util.honey-sql-2 :as h2x]
   [metabase.util.i18n :refer [tru]]
   [metabase.util.log :as log]
   [metabase.util.malli :as mu])
  (:import
   (com.aliyun.odps Odps OdpsException)
   (com.aliyun.odps.account AliyunAccount)
   (java.sql Connection ResultSet)
   (java.time Instant LocalDate LocalDateTime LocalTime OffsetDateTime OffsetTime ZonedDateTime)))

(set! *warn-on-reflection* true)

(defn- resolve-compat-var
  [& symbols]
  (or (some (fn [symbol]
              (try
                (requiring-resolve symbol)
                (catch java.io.FileNotFoundException _
                  nil)))
            symbols)
      (throw (ex-info "No compatible Metabase API implementation found"
                      {:symbols symbols}))))

(defn- secret-value-as-string
  [driver details property-name]
  (if-let [driver-api-fn
           (try
             (requiring-resolve 'metabase.driver-api.core/secret-value-as-string)
             (catch java.io.FileNotFoundException _
               nil))]
    (driver-api-fn driver details property-name)
    ((resolve-compat-var
       'metabase.models.secret/get-secret-string
       'metabase.secrets.models.secret/get-secret-string)
     details
     property-name)))

(defn- dispatch-by-clause-name-or-class
  [clause]
  ((resolve-compat-var
     'metabase.driver-api.core/dispatch-by-clause-name-or-class
     'metabase.legacy-mbql.util/dispatch-by-clause-name-or-class)
   clause))

(defn- setting-get-value-of-type
  [value-type setting-name]
  ((resolve-compat-var
     'metabase.driver-api.core/setting-get-value-of-type
     'metabase.models.setting/get-value-of-type)
   value-type
   setting-name))

(defn- add-optional-method!
  [multifn-symbol dispatch-value method]
  (when-let [multifn-var (find-var multifn-symbol)]
    (when (bound? multifn-var)
      (.addMethod ^clojure.lang.MultiFn @multifn-var dispatch-value method))))

(driver/register! :maxcompute, :parent :sql-jdbc)
(doseq [[feature supported?] {;; Does this database track and enforce primary key and foreign key constraints in the schema?
                              ;; SQL query engines like Presto and Athena do not track these, though they can query across FKs.
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
                              :standard-deviation-aggregations        true

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
                              :binning                                true

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
                              :full-join                              true

                              :regex                                  true

                              ;; Does the driver support advanced math expressions such as log, power, ...
                              :advanced-math-expressions              true

                              ;; Does the driver support percentile calculations (including median)
                              :percentile-aggregations                true

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
                              :convert-timezone                       true

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
           (let [{:keys [project endpoint ak namespace-schema]} details
                 account (AliyunAccount. ak (secret-value-as-string :maxcompute details "sk"))
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

;; SQL that Metabase shows in the UI / query log goes through sql-formatter
;; (`metabase.driver.sql.util/dialects`). There is no :maxcompute entry, so it
;; falls back to StandardSql, which does not understand backtick identifiers and
;; renders `tbl`.`col` as ` tbl `.` col ` — SQL that the engine rejects with
;; ODPS-0130071 "column ... cannot be resolved" if a user copies it out of the UI.
;; MaxCompute's quoting is MySQL-style, so format with that dialect.
(add-optional-method!
  'metabase.driver/prettify-native-form
  :maxcompute
  (fn [_driver s] (sql.u/format-sql :mysql s)))

;; 2026-09-08 engine-audit fix: without this, grouping by Week throws
;; "No method in multimethod 'db-start-of-week'". MaxCompute DATETRUNC 'week'
;; is Monday-based, which is Metabase's default start of week.
(defmethod driver/db-start-of-week :maxcompute
  [_]
  :monday)

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
                 sk-value (secret-value-as-string :maxcompute details-map "sk")
                 settings-map (merge default-settings (try
                                                        (when settings
                                                              (json/parse-string settings))
                                                        (catch Exception _
                                                          (log/warn "Ignoring invalid MaxCompute settings JSON.")
                                                          {})))]
                (if (or (nil? endpoint) (nil? project) (nil? ak) (nil? sk-value))
                  (throw (IllegalArgumentException. "Missing required connection details"))
                  {:classname   "com.aliyun.odps.jdbc.OdpsDriver"
                   :subprotocol "odps"
                   :subname     (str endpoint "?project=" project
                                     "&enableOdpsLogger=true&charset=UTF-8&interactiveMode=true&enableLimit=false&legacyArrayGetObject=false"
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
(defn ^:private project-id-for-current-query
      []
      (when (qp.store/initialized?)
            (when-let [{:keys [details]} (lib.metadata/database (qp.store/metadata-provider))]
                      (:project details))))

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
          dispatch-by-clause-name-or-class
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
              [target-type (dispatch-by-clause-name-or-class x)])
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

;; 2026-09-08 engine-audit fix (read-only tests on live project demo_project):
;; MaxCompute does NOT accept 'minute'/'second' as DATETRUNC/DATEADD datepart
;; tokens — only 'mi'/'ss' style short tokens, plus 'dd','hh','mm','yyyy',
;; 'day','month','year','week'. The 3-arg (timezone) form of DATETRUNC is also
;; invalid here (ODPS-0130121) — session tz comes from the connection settings.
(def ^:private mbql-unit->datepart-token
  "MBQL unit -> MaxCompute datepart token. NOTE: :quarter maps to 'quarter',
  which is valid for DATEADD (engine-verified) but NOT for DATETRUNC — the
  [:maxcompute :quarter] date method never routes through `trunc`, it builds
  the month-shift composite instead."
  {:second "ss" :minute "mi" :hour "hh" :day "dd"
   :month "mm" :year "yyyy" :week "week" :quarter "quarter"})

(defn- format-trunc
       [_tag [expr unit _report-timezone :as _args]]
       ;; report-timezone arg ignored: MaxCompute DATETRUNC has no 3-arg form.
       (let [t         (or (temporal-type expr) :datetime)
             f         (case t
                             :date      :datetrunc
                             :time      :datetrunc
                             :datetime  :datetrunc
                             :timestamp :datetrunc
                             :timestamp_ntz :datetrunc)
             token     (get mbql-unit->datepart-token unit (name unit))]
            (sql/format-expr [f expr (h2x/literal token)] {:nested true})))

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

;; 2026-09-08 engine-audit fix: `EXTRACT(dayofweek FROM x)` etc. are parse
;; errors on MaxCompute (invalid token 'FROM') — only a fixed spell set works
;; (day/month/…). dayofweek/dayofyear/isoweek must be computed via composites.
;; Also the 3-arg EXTRACT(x, tz) form is invalid on this engine.
;; -> temp: keep `extract*` for the supported spell set only. The unsupported
;; units are rerouted by the [:maxcompute …] `sql.qp/date` methods below.
(defn- format-extract
       [_tag [unit expr _timezone]]
       ;; timezone arg ignored: 3-arg EXTRACT is invalid on MaxCompute.
       (let [[expr-sql & expr-args] (sql/format-expr expr {:nested true})]
            (into [((clojure.core/format "EXTRACT(%s FROM %s)" (name unit) expr-sql))]
                  cat
                  [expr-args])))

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

;; 2026-09-08 engine-audit fix: rewritten against engine-verified token set
;; (live demo_project). Changed units carry a note; unchanged ones were OK.
(defmethod sql.qp/date [:maxcompute :second-of-minute] [_ _ expr] (extract :second    expr))
(defmethod sql.qp/date [:maxcompute :minute]           [_ _ expr] (trunc   :minute    expr)) ; 'minute'->'mi' in format-trunc (was 0130071)
(defmethod sql.qp/date [:maxcompute :minute-of-hour]   [_ _ expr] (extract :minute    expr))
(defmethod sql.qp/date [:maxcompute :hour]             [_ _ expr] (trunc   :hour      expr)) ; 'hour' token OK
(defmethod sql.qp/date [:maxcompute :hour-of-day]      [_ _ expr] (extract :hour      expr))
(defmethod sql.qp/date [:maxcompute :day]              [_ _ expr] (trunc   :day       expr))
(defmethod sql.qp/date [:maxcompute :day-of-month]     [_ _ expr] (extract :day       expr))
;; 'dayofyear' is not an EXTRACT spelling on MaxCompute (parse error) —
;; engine-verified composite: DATEDIFF(x, year-trunc(x), 'dd') + 1.
(defmethod sql.qp/date [:maxcompute :day-of-year]
           [_driver _unit expr]
           (h2x/+ [:datediff expr (trunc :year expr) (h2x/literal "dd")] [:inline 1]))
(defmethod sql.qp/date [:maxcompute :month]            [_ _ expr] (trunc   :month     expr))
(defmethod sql.qp/date [:maxcompute :month-of-year]    [_ _ expr] (extract :month     expr))
;; 'quarter' is NOT a DATETRUNC token (0130071). Re-engine-verified 2026-10-22
;; against the live project through the driver's own generated SQL:
;;   * DATEADD takes a BIGINT shift — integer `/` yields DOUBLE on this engine
;;     (ODPS-0130071 "dateadd cannot match any overloaded functions with
;;     (DATETIME, DOUBLE, STRING)"), so the shift is CAST(... AS BIGINT);
;;   * the back-off is `-((month - 1) % 3)` months, NOT `(month-1)/3*3`: with
;;     the old arithmetic Oct landed on Jan 1 instead of Oct 1;
;;   * `[:*-1 ...]` is not a HoneySQL operator — it rendered as `*-1(...)`, an
;;     invalid function call. Use `[:* [:inline -1] ...]`.
;; Verified: Oct/Dec -> Oct 1, Jun -> Apr 1, Jan -> Jan 1.
(defmethod sql.qp/date [:maxcompute :quarter]
           [_driver _unit expr]
           [:dateadd
            (trunc :month expr)
            (h2x/cast :bigint
                      (h2x/* [:inline -1]
                             (h2x/mod (h2x/- [:datepart expr (h2x/literal "mm")] [:inline 1])
                                      [:inline 3])))
            (h2x/literal "mm")])
(defmethod sql.qp/date [:maxcompute :quarter-of-year]
           [_driver _unit expr]
           (h2x/cast :bigint (h2x// (h2x/+ [:datepart expr (h2x/literal "mm")] [:inline 2]) [:inline 3])))
(defmethod sql.qp/date [:maxcompute :year]             [_ _ expr] (trunc   :year      expr))
(defmethod sql.qp/date [:maxcompute :year-of-era]      [_ _ expr] (extract :year      expr))

;; 2026-09-08 engine-audit fix: there is NO `mod(x,y)` function on MaxCompute
;; (ODPS-0130071) — the `%` operator is the only remaining form (A9/A10 audit).
;; The day-of-week method below switched from the (removed) ::mod clause to
;; h2x/mod, which renders the `%` operator.

;; `EXTRACT(dayofweek FROM x)` is a parse error on MaxCompute and
;; `DATEPART(x, 'dayofweek')` is 0130071, so day-of-week is a WEEKDAY composite.
;; WEEKDAY(x) = Mon=0..Sun=6, and `adjust-day-of-week` requires the value the
;; driver hands it to be 1..7 with 1 = `db-start-of-week` — which this driver
;; declares as :monday. Hence `((WEEKDAY(x) % 7) + 1)`: Mon=1 … Sun=7.
;;
;; WEEKDAY accepts DATETIME only — on a TIMESTAMP argument the engine raises
;; ODPS-0130121 "invalid type TIMESTAMP of argument 1 for function weekday,
;; expect DATETIME", which broke every day-of-week bucket on timestamp-typed or
;; unix-ms-coerced columns. Cast first (verified 2026-10-22 through the driver's
;; own generated SQL: Fri=5, Sun=7, Mon=1 after the start-of-week shift,
;; Fri=6 under the default Sunday start-of-week).
(defmethod sql.qp/date [:maxcompute :day-of-week]
           [driver _ expr]
           (sql.qp/adjust-day-of-week
             driver
             (h2x/+ (h2x/mod [:weekday (h2x/cast :datetime expr)] [:inline 7]) [:inline 1])
             (driver.common/start-of-week-offset driver)
             h2x/mod))

;; 2026-09-08 review fix: :week now honours the instance's start-of-week
;; setting. Native DATETRUNC 'week' is always Monday-based (G/O audits), and
;; engine-verified 'week(sunday)' / the O4 shift-composite exist for the
;; Sunday-start case ('week(sunday)' audit O3). Using the setting-aware
;; spelling keeps bucket edges aligned when the admin changes start-of-week.
(defmethod sql.qp/date [:maxcompute :week]
           [_driver _unit expr]
           (trunc (keyword (format "week(%s)"
                                   (name (driver/db-start-of-week :maxcompute))))
                  expr))

;; 'isoweek' is not an EXTRACT spelling on MaxCompute (parse error) and
;; DATEPART 'isoweek' returns 0 (wrong) — WEEKOFYEAR matches ISO week on all
;; probed edge dates (J audit: 2027-01-01→53, 2026-12-28→53, 2026-01-01→1).
(defmethod sql.qp/date [:maxcompute :week-of-year-iso]
           [_driver _unit expr]
           [:weekofyear expr])

;; 2026-09-07/08 unix-timestamp fix (engine-verified, see PR #2 + skill notes):
;; previous impl emitted nonexistent `TIMESTAMP_MILLIS/SECONDS/MICROS(x)` built-ins
;; (ODPS-0130071 live), and the :sql :milliseconds default leaked
;; `FROM_UNIXTIME(x / 1000.0)` into generated SQL: `/` on integers returns DOUBLE
;; and FROM_UNIXTIME only takes BIGINT → ODPS-0130121 (the visible-query-builder bug).
;;
;; Engine-verified replacements (all read-only tested 2026-09-07):
;;   :seconds      → CAST(FROM_UNIXTIME(CAST(x AS BIGINT)) AS TIMESTAMP)   — exact
;;   :milliseconds → CAST(CONCAT(TO_CHAR(FROM_UNIXTIME(CAST(x / 1000 AS BIGINT)),'yyyy-mm-dd hh:mi:ss'),
;;                             '.', LPAD(CAST(x % 1000 AS STRING), 3, '0')) AS TIMESTAMP)
;;                   CAST(x / 1000 AS BIGINT) truncates toward zero == DIV; `%` keeps ms.
;;   :microseconds → same shape with 1e6/6-digit padding.
;; CAST(DATETIME AS TIMESTAMP) and CAST(STRING AS TIMESTAMP) (fractional seconds ok)
;; are both supported conversions; MOD must use the `%` operator (MaxCompute has no
;; MOD function — ODPS-0130071 on `mod(x,y)`).
(defn- unix-s->honeysql
  [expr]
  (h2x/cast :timestamp [:from_unixtime (h2x/cast :bigint expr)]))

(defn- split-unix-n
  "Shared shape for ms/µs: seconds via BIGINT-truncating division (`CAST(x / N AS BIGINT)`
  truncates toward zero, same as DIV), remainder via `%`; recombined as a
  'yyyy-mm-dd hh:mi:ss.f{digits}' string and cast to TIMESTAMP. `digits` is 3 (ms) or 6 (µs)."
  [expr divisor digits]
  (let [divisor-expr [:inline divisor]
        seconds      (h2x/cast :bigint (h2x// expr divisor-expr))
        frac         (h2x/mod expr divisor-expr)
        datetime-str [:concat
                      [:to_char [:from_unixtime seconds] (h2x/literal "yyyy-mm-dd hh:mi:ss")]
                      (h2x/literal ".")
                      [:lpad [:cast frac :string] [:inline digits] (h2x/literal "0")]]]
    (h2x/cast :timestamp datetime-str)))

(doseq [[unix-timestamp-type impl-fn] {:seconds      unix-s->honeysql
                                       :milliseconds #(split-unix-n % 1000 3)
                                       :microseconds #(split-unix-n % 1000000 6)}]
       (defmethod sql.qp/unix-timestamp->honeysql [:maxcompute unix-timestamp-type]
                  [_driver _unix-timestamp-type expr]
                  (-> (impl-fn expr)
                      (with-temporal-type :timestamp)
                      (h2x/with-database-type-info "timestamp")
                      (with-temporal-type :timestamp))))

;; 2026-09-08 engine-audit fix: `DATETIME(x, tz)` does NOT exist on MaxCompute
;; (ODPS-0130071, E9 audit). Engine-verified path: TIMESTAMP(x, tz) works
;; (E8), CAST(TIMESTAMP(x, 'UTC') AS datetime) shifts the wall clock (S1):
;; from 12:34 → 20:34 with Asia/Shanghai target.
;; MBQL 5 may carry a clause metadata map as the first argument (e.g. a
;; percentile aggregation or expression renamed in the GUI arrives as
;; [:percentile {name ...} expr 0.9]). Any custom ->honeysql implementation that
;; destructures positional arguments must strip those maps first, otherwise the
;; map is handed to ->honeysql and blows up with "Unexpected ->honeysql call on a
;; map" (reproduced in production 2026-10-22).
(defn- drop-clause-metadata
       [clause]
       (remove map? (rest clause)))

(defmethod sql.qp/->honeysql [:maxcompute :convert-timezone]
           [driver clause]
           (let [[arg target-timezone source-timezone] (drop-clause-metadata clause)
                 hsql-form    (sql.qp/->honeysql driver arg)
                 timestamptz? (h2x/is-of-type? hsql-form "timestamp")]
                (sql.u/validate-convert-timezone-args timestamptz? target-timezone source-timezone)
                (as-> (if timestamptz?
                        hsql-form
                        [:timestamp hsql-form (h2x/literal (or source-timezone (qp.timezone/results-timezone-id)))]) form
                    [:cast
                     [:timestamp form (h2x/literal target-timezone)]
                     :datetime]
                    (with-temporal-type form :datetime))))

;; 2026-09-08 engine-audit fix: MaxCompute has no `float64` type (ODPS-0130071, E1
;; audit); DOUBLE is the native spelling (E2).
(defmethod sql.qp/->float :maxcompute
           [_ value]
           (h2x/cast :double value))

;; 2026-09-08 engine-audit fix: MaxCompute has no `float64` type (ODPS-0130071);
;; DOUBLE is the native spelling. Registered for cores that took `->float` away
;; in favour of `float-dbtype` (the `->float` defmethod above covers older cores).
(add-optional-method!
  'metabase.driver.sql.query-processor/float-dbtype
  :maxcompute
  (fn [_driver] :double))

;; `LIKE` on MaxCompute has a built-in backslash escape and does NOT accept the
;; SQL-standard ESCAPE clause — `... LIKE '%2026%' ESCAPE '\'` is ODPS-0130161
;; "invalid token", and `ESCAPE '!'` fails the same way. Engine-verified that the
;; escaping itself works without the clause:
;;   'a%b' LIKE 'a\%b' -> true      'axb' LIKE 'a\%b' -> false
;;   'a_b' LIKE 'a\_b' -> true      'axb' LIKE 'a\_b' -> false
;; Core 0.59+ adds the clause via transform-literal-like-pattern-honeysql, which
;; broke every :contains / :starts-with / :ends-with filter (found by the
;; 2026-10-22 sweep). Drop the wrapper for this driver.
(add-optional-method!
  'metabase.driver.sql.query-processor/transform-literal-like-pattern-honeysql
  :maxcompute
  (fn [_driver like-rhs-honeysql] like-rhs-honeysql))

(defmethod sql.qp/->honeysql [:maxcompute :regex-match-first]
           [driver clause]
           (let [[arg pattern] (drop-clause-metadata clause)]
                [:regexp_extract (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern)]))

(defn- format-percentile
       [_tag [expr p :as _args]]
       (let [[expr-sql & expr-args] (sql/format-expr expr {:nested true})
             [p-sql & p-args]       (sql/format-expr p {:nested true})]
            (into [(format "PERCENTILE(%s, %s)" expr-sql p-sql)]
                  cat
                  [expr-args p-args])))

(sql/register-fn! ::percentile #'format-percentile)

;; 2026-10-22 engine-verified (live demo_project): PERCENTILE(x, p) is the native
;; form and takes p as a 0..1 fraction — 1..10 gives p50=5.5 / p90=9.1, and
;; MEDIAN(x) == PERCENTILE(x, 0.5). The previous implementation emitted
;; APPROX_QUANTILES(x, n)[OFFSET(k)], a BigQuery function this engine does not
;; have (ODPS-0130071; the `odps.sql.bigquery.compatible` connection setting does
;; not enable it either — SAFE_DIVIDE fails the same way), so the code path was
;; dead while the feature flag kept it out of the UI.
;; MBQL 5 may carry a clause metadata map as the first argument — a percentile
;; aggregation renamed in the GUI arrives as
;;   [:percentile {:name "input_p90" :display-name "input_p90" ...} expr 0.9]
;; Core's [:sql :percentile] does not expect that shape either, so passing the map
;; straight to ->honeysql failed with "Unexpected ->honeysql call on a map"
;; (reproduced 2026-10-22 through /api/dataset/native on the production instance).
;; Drop any map arguments before destructuring.
(defmethod sql.qp/->honeysql [:maxcompute :percentile]
           [driver clause]
           (let [[expr p] (drop-clause-metadata clause)]
                [::percentile (sql.qp/->honeysql driver expr)
                 (if (number? p) [:inline (double p)] p)]))

(defmethod sql.qp/->honeysql [:maxcompute :median]
           [driver clause]
           (let [[arg] (drop-clause-metadata clause)]
                (sql.qp/->honeysql driver [:percentile arg 0.5])))


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

         ;; Fields rendered inside a source query / join are still real columns of a
         ;; real table until an alias takes over; MaxCompute only resolves a column
         ;; qualifier when the full `<project>.<schema>.<table>` is quoted as ONE
         ;; identifier (`a.b.c`.`col`) — `schema`.`table`.`col` fails with
         ;; ODPS-0130071 "column ... cannot be resolved" (verified on the production
         ;; project 2026-10-22). Aliased fields carry two components ([alias col]) and
         ;; stay unqualified; three-or-more components mean a real table, qualify it.
         (and (= identifier-type :field)
              (>= (count components) 3))
         true

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
           [:to_date expr (h2x/literal "yyyymmddhhmiss")])

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
           [driver clause]
           (let [[x y unit] (drop-clause-metadata clause)
                 x (sql.qp/->honeysql driver x)
                 y (sql.qp/->honeysql driver y)]
                (datetime-diff-check-args x y)
                (sql.qp/datetime-diff driver unit x y)))

;; 2026-09-08 engine-audit fixes — datetime-diff family (Z/AA audit):
;; TIMESTAMP_DIFF / DATETIME_DIFF built-ins DO NOT exist on MaxCompute
;; (ODPS-0130071). DATEDIFF(x, y, token) is the engine-verified native
;; form (accepts DATETIME or TIMESTAMP args, AA1 audit) with short tokens
;; 'dd','hh','mi','ss','mm','yyyy'. Month semantics: calendar-month
;; boundaries; whole months = subtract 1 when day(x) < day(y) AND x>=y
;; (Z1: 22→21; Z-negative dir unchanged: -22 for x<y dir, Z5/Z6 audit).
(defmethod sql.qp/datetime-diff [:maxcompute :year]
           [driver _unit x y]
           (h2x/cast :bigint (h2x// (sql.qp/datetime-diff driver :month x y) [:inline 12])))
(defmethod sql.qp/datetime-diff [:maxcompute :quarter]
           [driver _unit x y]
           (h2x/cast :bigint (h2x// (sql.qp/datetime-diff driver :month x y) [:inline 3])))
(defmethod sql.qp/datetime-diff [:maxcompute :month]
           [_driver _unit x y]
           (h2x/- [:datediff x y (h2x/literal "mm")]
                  [:case
                   [:< [:datepart x (h2x/literal "dd")] [:datepart y (h2x/literal "dd")]] [:inline 1]
                   :else [:inline 0]]))
(defmethod sql.qp/datetime-diff [:maxcompute :week]
           [driver _unit x y]
           (h2x/cast :bigint (h2x// (sql.qp/datetime-diff driver :day x y) [:inline 7])))
(defmethod sql.qp/datetime-diff [:maxcompute :day]
           [_driver _unit x y]
           [:datediff (trunc :day x) (trunc :day y) (h2x/literal "dd")])
(defmethod sql.qp/datetime-diff [:maxcompute :hour]
           [_driver _unit x y]
           [:datediff x y (h2x/literal "hh")])
(defmethod sql.qp/datetime-diff [:maxcompute :minute]
           [_driver _unit x y]
           [:datediff x y (h2x/literal "mi")])
(defmethod sql.qp/datetime-diff [:maxcompute :second]
           [_driver _unit x y]
           [:datediff x y (h2x/literal "ss")])

(defn- crc32-hex
  "CRC32 of `s`'s UTF-8 bytes as 8-char zero-padded hex, e.g. `30de4302`.
   Mirrors lib/util/unique-name-generator's `crc32-checksum` (same algorithm core
   uses for truncated aliases), re-implemented here so the driver stays self-contained
   across metabase-core versions."
  ^String [^String s]
  (let [raw (Long/toHexString (.getValue (doto (java.util.zip.CRC32.)
                                           (.update (.getBytes s "UTF-8")))))]
    (loop [r raw]
      (if (< (count r) 8)
        (recur (str \0 r))
        r))))

(defn- collision-safe-escape-alias
  "Escape `s` to a valid MaxCompute identifier; when the fold is LOSSY (it actually
   replaced characters), also append the original string's CRC32.

   Why: core add-alias-info uniquifies join aliases BEFORE escaping
   (`(comp escape-fn unique-name-generator)` — uniquify first, escape second, in
   that order). Distinct non-ASCII aliases like 「用户个人认证记录 - user_id」 and
   「用户累计其他消费 - user_id」 both fold to underscore mush like `___________user_id`,
   so distinct joins collide and MaxCompute rejects the SQL with ODPS-0130071
   (`user_id is ambiguous`). Core is unaware of the collision because its uniquify
   pass ran on the PRE-fold names; upstream master still has this order today.
   Appending a hash of the ORIGINAL (pre-fold) string makes post-fold results
   distinct again, deterministically, for any pair of distinct originals.

   Lossless folds keep <= v0.0.7 behavior byte-for-byte (no suffix), so existing
   queries compile to exactly the same SQL as before."
  [driver s]
  (let [normalized (-> (str/trim s)
                       u/remove-diacritical-marks)
        folded    (-> normalized
                      (str/replace #"[^\w\d_]" "_")
                      (str/replace #"(^\d)" "_$1"))]
    (if (= folded s)
      ;; lossless fold: the identifier equals the original string unchanged —
      ;; identical behavior to v0.0.7 and earlier, byte-for-byte.
      ((get-method driver/escape-alias :metabase.driver/driver) driver folded)
      ;; LOSSY fold (trim, diacritics, char replacement, or digit prefix — any
      ;; change vs the ORIGINAL string): two distinctly-written originals can
      ;; collide onto the same identifier (e.g. 「用户个人认证记录 - user_id」 vs
      ;; 「用户累计其他消费 - user_id」, or 「café」 vs 「cafe」, or 「9x」 vs
      ;; 「_9x」). Disambiguate with a deterministic hash of the original.
      (let [hashed (str folded \_ (crc32-hex s))]
        ((get-method driver/escape-alias :metabase.driver/driver) driver hashed)))))

(defmethod driver/escape-alias :maxcompute
           [driver s]
           ;; Convert field alias `s` to a valid MaxCompute field identifier. From the dox: Fields must contain only letters,
           ;; numbers, and underscores, start with a letter or underscore, and be at most 128 characters long.
           (collision-safe-escape-alias driver s))


;;; |                                             inline-value impls                                                |
;;; +----------------------------------------------------------------------------------------------------------------+
;;; sql.qp/inline-value is the mechanism for inlining temporal literals when *compile-with-inline-parameters*
;;; is true (used for native query compilation and certain prepared-statement fallback paths). It replaced the
;;; legacy `unprepare/unprepare-value` multimethod, which up to 0.1.1 this driver also implemented.

(defmethod sql.qp/inline-value [:maxcompute String]
           [_ s]
           ;; escape single-quotes like Cam's String -> Cam\'s String.
           ;; Engine-verified: `'Cam\'s String'` returns Cam's String, while the
           ;; doubled-backslash form upstream shipped (Cam\\'s String) breaks the
           ;; literal, and SQL-standard doubled quotes ('it''s') silently evaluate
           ;; to a literal concatenation ("its") rather than escaping the quote.
           (str \' (str/replace s "'" "\\'") \'))

;; Temporal literal forms, engine-verified 2026-10-22 on the production project:
;;   datetime"2026-10-15 23:00:00"        -> OK   (wall clock)
;;   date"2026-10-15"                     -> OK
;;   timestamp"2026-10-15 23:00:00"       -> OK
;;   timestamp"2026-10-15 23:00:00Z"      -> OK   (Z is honoured: 23:00Z = 07:00+08:00)
;;   timestamp"2026-10-16 07:00:00+08:00" -> OK but the offset is IGNORED (read as wall clock)
;;   datetime"2026-10-15 23:00:00Z"       -> ODPS-0130161 invalid DATETIME format
;;   datetime"2026-10-15 23:00:00+08:00"  -> ODPS-0130161 invalid DATETIME format
;;   datetime"23:00:00"                   -> ODPS-0130161 invalid DATETIME format
;; So: instants must be inlined as `timestamp"<UTC wall clock>Z"` (a `datetime`
;; literal may never carry a zone), and time-of-day values need a date anchor.
(defn- inline-utc-timestamp
       "A timestamp literal for an instant-like value: UTC wall clock plus a Z suffix."
       ^String [t]
       (let [utc (t/offset-date-time t (t/zone-offset 0))]
            (format "timestamp\"%sZ\"" (u.date/format-sql (t/local-date-time utc)))))

(defmethod sql.qp/inline-value [:maxcompute LocalTime]
           [_ t]
           ;; no TIME literal on this engine; anchor the wall clock to the epoch date
           (format "datetime\"1970-01-01 %s\"" (u.date/format-sql t)))

(defmethod sql.qp/inline-value [:maxcompute LocalDate]
           [_ t]
           (format "date\"%s\"" (u.date/format-sql t)))

(defmethod sql.qp/inline-value [:maxcompute LocalDateTime]
           [_ t]
           (format "timestamp\"%s\"" (u.date/format-sql t)))

(defmethod sql.qp/inline-value [:maxcompute Instant]
           [_ t]
           (inline-utc-timestamp t))

(defmethod sql.qp/inline-value [:maxcompute OffsetTime]
           [_ t]
           ;; convert to a LocalTime in UTC
           (let [local-time (t/local-time (t/with-offset-same-instant t (t/zone-offset 0)))]
                (format "datetime\"1970-01-01 %s\"" (u.date/format-sql local-time))))

(defmethod sql.qp/inline-value [:maxcompute OffsetDateTime]
           [_ t]
           (inline-utc-timestamp t))

(defmethod sql.qp/inline-value [:maxcompute ZonedDateTime]
           [_ t]
           (inline-utc-timestamp t))

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
       [_tag [target-type _report-timezone :as _args]]
       (sql/format-expr
         [:cast
          [:getdate]
          [:raw (case (or target-type :timestamp)
                  :time      "datetime"
                  :date      "date"
                  :datetime  "datetime"
                  :timestamp_ntz "datetime"
                  "timestamp")]]
         {:nested true}))

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
           [driver clause]
           (let [[field] (drop-clause-metadata clause)]
                [:log [:inline 10] (sql.qp/->honeysql driver field)]))

(defmethod sql.qp/quote-style :maxcompute
           [_driver]
           :mysql)

;; convert OffsetDateTime to an ZonedDateTime in UTC since MaxCompute doesn't handle ZonedDateTime as we'd like
;(defmethod driver.sql/->prepared-substitution [:maxcompute OffsetDateTime]
;           [driver t]
;           (driver.sql/->prepared-substitution driver (t/zoned-date-time result (t/zone-id "UTC"))))

(mu/defmethod sql.params.substitution/->replacement-snippet-info [:maxcompute :metabase.lib.parameters.parse.types/field-filter]
              [driver                            :- :keyword
               {:keys [field], :as field-filter} :- [:map
                                                     [:field ::lib.schema.metadata/column]]]
              (let [field-temporal-type (temporal-type field)
                    parent-method       (get-method sql.params.substitution/->replacement-snippet-info [:sql :metabase.lib.parameters.parse.types/field-filter])
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
           (h2x/->datetime [:replace [:replace expr (h2x/literal "T") (h2x/literal " ")]
                                     (h2x/literal "Z") (h2x/literal "")]))

(defmethod sql.qp/cast-temporal-string [:maxcompute :Coercion/ISO8601->Date]
           [_driver _semantic_type expr]
           (h2x/->date [:replace [:replace expr (h2x/literal "T") (h2x/literal " ")]
                                (h2x/literal "Z") (h2x/literal "")]))

;; 2026-09-08 review round-2 fix (engine probes): CAST(str AS datetime|timestamp)
;; on MaxCompute accepts ONLY 'yyyy-mm-dd hh:mi:ss[.fff]' — colon-milliseconds
;; ('hh:mi:ss:fff') and ' +0800' suffix silently return NULL. Probe-verified safe
;; universal form: SUBSTR to the canonical 19 chars, then cast.
(defmethod sql.qp/cast-temporal-string [:maxcompute :Coercion/String->Temporal]
           [_driver _coercion-strategy expr]
           (h2x/->datetime [:substr expr [:inline 1] [:inline 19]]))

;; No TIME type on MaxCompute — leave the generic cast (unsupported target).
(defmethod sql.qp/cast-temporal-string [:maxcompute :Coercion/ISO8601->Time]
           [_driver _semantic_type expr]
           (h2x/->time expr))
