(ns metabase.driver.maxcompute-test
  "Unit tests for the MaxCompute driver.

  These tests do not require a live MaxCompute connection — they exercise the
  pure-Clojure pieces (namespace loading, identifier escaping, temporal literal
  formatting, temporal-type inference, and MBQL→HoneySQL compilation)."
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [honey.sql :as sql]
   [java-time.api :as t]
   [metabase.driver :as driver]
   [metabase.driver.maxcompute :as maxcompute]
   [metabase.driver.sql.query-processor :as sql.qp]
   [metabase.driver.sql.util :as sql.u]
   [metabase.query-processor.compile :as qp.compile]
   [metabase.lib.core :as lib]
   [metabase.lib.test-util :as lib.tu]
   [metabase.lib.test-metadata :as lib.test-md]
   [metabase.query-processor.store :as qp.store]
   [metabase.query-processor.util.add-alias-info :as add]
   [metabase.util.date-2 :as u.date])
  (:import
   (java.time LocalDate LocalDateTime LocalTime OffsetDateTime OffsetTime ZonedDateTime Instant)))

(deftest ^:parallel namespace-loads-test
  (testing "The maxcompute namespace loads without throwing (validates ns declaration, requires, register!)"
    (is (some? (find-ns 'metabase.driver.maxcompute)))))

;; 2026-09-08 production incident (metabase.example.invalid v0.63.16 + driver v0.0.6):
;; table browse / simple data select rendered SQL with EMPTY select expressions
;; (…SELECT AS `_key_`…) and a one-blob FROM `project.db.table`. Guard the full
;; pipeline (field refs -> add-alias-info -> top-level clauses -> rendered SQL)
;; — unit-level HoneySQL shape tests missed this entirely.
(defn- compile-simple-table-query
  "Compiles an MBQL query (table browse shape: plain fields, no aggregation) to native
   SQL with a mock metadata provider whose Database details carry :project, mirroring
   the production incident (queryHash e0bfeb…). Returns the rendered SQL string."
  [db-details]
  (qp.store/with-metadata-provider
    (lib.tu/mock-metadata-provider
      {:database {:lib/type :metadata/database, :id 1 :name "test-mc" :engine :maxcompute
                  :details db-details}
       :tables   [{:lib/type :metadata/table, :id 100 :name "sample_events" :schema "demo"}]
       :fields   [{:lib/type :metadata/column, :id 1001 :name "_key_"      :table-id 100 :base-type :type/Text}
                  {:lib/type :metadata/column, :id 1002 :name "_timestamp_" :table-id 100
                   :base-type :type/BigInteger :effective-type :type/DateTime
                   :coercion-strategy :Coercion/UNIXMilliseconds->DateTime}]})
    (driver/with-driver :maxcompute
      (:query
       (qp.compile/compile
         {:database 1
          :type     :query
          :query    {:source-table 100
                   :fields      [[:field 1001 nil] [:field 1002 nil]]}})))))

(deftest integration-table-browse-sql-test
  (testing "table-browse MBQL query compiles to well-formed SQL: every field renders an expression, FROM is two-part"
    (let [sql (compile-simple-table-query {:project "demo_project" :endpoint "http://x" :ak "a" :sk "b"})]
      (testing "no empty SELECT expressions (the incident's `SELECT AS `_key_``)"
        (is (str/includes? sql "_key_"))
        (is (not (re-matches #"(?s).*SELECT +(AS|,).*" sql)) "select list must not be empty"))
      (testing "FROM identifier must not be a single-blob `project.db.table`"
        (is (not (str/includes? sql "`demo_project.demo_project.sample_events`")))))))

(deftest ^:parallel driver-registered-test
  (testing ":maxcompute is registered with :sql-jdbc parent (derives from :sql as well)"
    ;; `isa?` against driver/hierarchy is the structural check; `driver/initialized?` is a runtime
    ;; state check that requires `driver/initialize!` to have run, which doesn't happen in unit tests.
    ;; 2026-09-08 incident fix: the driver must NOT derive from :sql-mbql5 —
    ;; its clause-order forwarding shims corrupt legacy 3-tuple :field clauses
    ;; on v0.63.x (identifiers render with empty components; the metabase.example.invalid
    ;; production breakage). See register comment in maxcompute.clj.
    (is (isa? driver/hierarchy :maxcompute :sql-jdbc))
    (is (isa? driver/hierarchy :maxcompute :sql))
    (is (not (isa? driver/hierarchy :maxcompute :sql-mbql5))
        ":maxcompute must not derive from :sql-mbql5 (legacy-field reorder bug)")))

(def ^:private escape-alias-lossless-cases
  "Inputs whose fold is lossless: escaped identifier == input unchanged (v0.0.7
   behavior, byte-for-byte; no hash suffix)."
  ["simple" "with_space" "_9starts_with_digit" "padded" "cafe" "emoji_name"])

(deftest ^:parallel escape-alias-test
  (testing "lossless folds are returned unchanged (v0.0.7 behavior, no hash suffix)"
    (doseq [input escape-alias-lossless-cases]
      (is (= input (driver/escape-alias :maxcompute input))
          (str input " should roundtrip unchanged"))))

  (testing "lossy folds are folded + `_` + crc32(original)"
    (are [input folded] (let [escaped (driver/escape-alias :maxcompute input)]
                          (and (str/starts-with? escaped (str folded \_))
                               (= 9 (- (count escaped) (count folded)))))
      "with space"         "with_space"
      "café"               "cafe"
      "9starts-with-digit" "_9starts_with_digit"
      "emoji🚀name"        "emoji_name"
      "  padded  "         "padded")
    (testing "concrete CRC32 values are stable (golden values, computed from SQL reference impl)"
      (is (= "cafe_98ad42b5" (driver/escape-alias :maxcompute "café"))))

    (testing "every lossy fold ends in 8 lowercase hex chars"
      (doseq [input ["with space" "café" "9starts-with-digit" "emoji🚀name" "  padded  "]]
        (let [escaped (driver/escape-alias :maxcompute input)]
          (is (re-matches #".*_[0-9a-f]{8}$" escaped)
              (str (pr-str input) " -> " (pr-str escaped) " lacks CRC32 suffix")))))))

;; 2026-09-08 second production incident (metabase.example.invalid v0.63.16 + driver
;; v0.0.7): the 6-join production card compiles fine, but core add-alias-info uniquifies
;; join aliases BEFORE escaping, so THREE distinct Chinese aliases
;; (个人认证记录/机构认证信息/累计其他消费) all folded to `___________user_id` and
;; MaxCompute rejected the SQL with ODPS-0130071 (ambiguous column). This test
;; compiles a production-shaped 6-join query end-to-end and asserts per-join
;; escape uniqueness at the SQL level.
(defn- compile-6-join-query
  "Compiles an MBQL query shaped like the production card (source table + 6 left joins onto
   sibling tables, each join pulling one field) to native SQL. Join aliases mirror
   production exactly."
  []
  (let [mp (lib.tu/mock-metadata-provider
             {:database {:lib/type :metadata/database, :id 1 :name "test-mc" :engine :maxcompute
                         :details {:project "demo_project" :endpoint "http://x" :ak "a" :sk "b"}}
              :tables   [{:lib/type :metadata/table, :id 100 :name "users" :schema "demo"}
                         {:lib/type :metadata/table, :id 101 :name "personal_verify"}
                         {:lib/type :metadata/table, :id 102 :name "org_verify"}
                         {:lib/type :metadata/table, :id 103 :name "charge_refund_stats"}
                         {:lib/type :metadata/table, :id 104 :name "maas_spending"}
                         {:lib/type :metadata/table, :id 105 :name "faas_spending"}
                         {:lib/type :metadata/table, :id 106 :name "other_expense"}]
              :fields   (into [{:lib/type :metadata/column, :id 1001 :name "user_id" :table-id 100 :base-type :type/Text}]
                              (mapcat (fn [[tid fld]]
                                        [{:lib/type :metadata/column, :id (+ 2000 tid)
                                          :name "user_id" :table-id tid :base-type :type/Text}
                                         {:lib/type :metadata/column, :id (+ 2100 tid)
                                          :name fld :table-id tid :base-type :type/Text}])
                                      {100 "user_id" 101 "personal_user_id" 102 "org_user_id" 103 "stats_user_id"
                                       104 "maas_user_id" 105 "faas_user_id" 106 "other_user_id"}))})]
    (qp.store/with-metadata-provider mp
      (driver/with-driver :maxcompute
        (:query
          (qp.compile/compile
            {:database 1
             :type     :query
             :lib/metadata mp
             :query    {:source-table 100
                        :fields [[:field 1001 nil]]
                        :joins   (mapv (fn [tid alias join-field-id]
                                         {:strategy     :left-join
                                          :alias        alias
                                          :source-table tid
                                          :condition    [:= [:field 1001 nil] [:field join-field-id nil]]
                                          :fields       [[:field join-field-id nil]]})
                                       [101 102 103 104 105 106]
                                       ["用户个人认证记录 - user_id"
                                        "用户机构认证信息 - user_id"
                                        "用户充值退款累计统计 - user_id"
                                        "用户 MaaS 累计消费 - user_id"
                                        "用户 FaaS 累计消费 - user_id"
                                        "用户累计其他消费 - user_id"]
                                       [2101 2102 2103 2104 2105 2106])}}))))))


(deftest integration-6-join-alias-uniqueness-test
  (testing "end-to-end: a card-432-shaped 6-join query compiles with pairwise-distinct join aliases"
    (let [sql (compile-6-join-query)]
      (testing "all six LEFT JOINs are present"
        (is (= 6 (count (re-seq #"(?i)LEFT JOIN" sql)))))
      (testing "every join alias carries a CRC32 suffix (no bare underscore-mush aliases)"
        (is (not (re-find #"`_{11}user_id`" sql))
            "collided alias _______user_id (11 underscores) must not appear verbatim"))
      (testing "escaped alias count: every LEFT JOIN introduces its own aliased subquery"
        (is (<= 6 (count (re-seq #"[0-9a-f]{8}" sql)))
            "expected six CRC32 suffixes (plus any from folded comparison predicates)")))))

(deftest ^:parallel escape-alias-disambiguation-test
  (testing "distinct non-ASCII aliases that fold identically escape to distinct identifiers
            (production incident: 6-join card, ODPS-0130071 ambiguous join alias)"
    (let [prod-aliases ["用户个人认证记录 - user_id"
                        "用户机构认证信息 - user_id"
                        "用户充值退款累计统计 - user_id"
                        "用户 MaaS 累计消费 - user_id"
                        "用户 FaaS 累计消费 - user_id"
                        "用户累计其他消费 - user_id"]
          escaped      (mapv #(driver/escape-alias :maxcompute %) prod-aliases)]
      (testing "under the OLD v0.0.7 logic these collapsed onto shared underscore mush"
        ;; the six fold into only 4 distinct identifiers: the three 8-char
        ;; Chinese prefixes (个人认证记录/机构认证信息/累计其他消费) collapse onto
        ;; `___________user_id` — that triple collision is the incident.
        (is (= 4 (count (distinct (map #(str/replace % #"[^\w\d_]" "_")
                                       prod-aliases))))))
      (testing "with the hash suffix, all six escaped values are pairwise distinct"
        (is (= (count escaped) (count (distinct escaped)))
            (str "escaped aliases must be unique, got: " (pr-str escaped))))
      (testing "each escaped value is a MaxCompute-valid identifier: ^[A-Za-z_][A-Za-z0-9_]*$"
        (doseq [e escaped]
          (is (re-matches #"[A-Za-z_][A-Za-z0-9_]*" e)
              (str (pr-str e) " is not a valid MaxCompute identifier")))))))

(deftest ^:parallel inline-value-string-test
  (testing "inline-value for String returns a quoted SQL string literal with escaped single quotes"
    (is (= "'Cam\\'s String'"
           (sql.qp/inline-value :maxcompute "Cam's String")))))

(deftest ^:parallel inline-value-temporal-test
  (testing "inline-value formats temporal literals with MaxCompute-specific syntax"
    (let [local-date     (LocalDate/of 2024 1 15)
          local-time     (LocalTime/of 12 30 45 0)
          local-datetime (LocalDateTime/of 2024 1 15 12 30 45 0)
          instant        (Instant/parse "2024-01-15T12:30:45Z")
          offset-time    (OffsetTime/of 12 30 45 0 (t/zone-offset 8))]
      (is (= (format "date\"%s\"" (u.date/format-sql local-date))
             (sql.qp/inline-value :maxcompute local-date)))
      ;; a bare datetime"hh:mm:ss" is ODPS-0130161 on this engine, so the wall
      ;; clock is anchored to the epoch date
      (is (= (format "datetime\"1970-01-01 %s\"" (u.date/format-sql local-time))
             (sql.qp/inline-value :maxcompute local-time)))
      (is (= (format "timestamp\"%s\"" (u.date/format-sql local-datetime))
             (sql.qp/inline-value :maxcompute local-datetime)))
      ;; instants are emitted as <UTC wall clock>Z (a datetime literal may not carry a zone)
      (is (= (format "timestamp\"%sZ\"" (u.date/format-sql (t/local-date-time (t/offset-date-time instant (t/zone-offset 0)))))
             (sql.qp/inline-value :maxcompute instant)))
      ;; OffsetTime is converted to LocalTime in UTC
      (is (str/starts-with? (sql.qp/inline-value :maxcompute offset-time) "datetime\"1970-01-01 ")))))

(deftest ^:parallel temporal-type-inference-test
  (testing "temporal-type correctly infers types from Java temporal values"
    ;; These call the private multimethod via var; use the public entry points if available.
    ;; We test indirectly through the metadata attached by with-temporal-type.
    (let [local-date (LocalDate/of 2024 1 1)]
      ;; LocalDate should be associated with :date
      (is (= :date (#'maxcompute/temporal-type local-date))))
    (let [local-datetime (LocalDateTime/of 2024 1 1 0 0 0)]
      (is (= :timestamp_ntz (#'maxcompute/temporal-type local-datetime))))))

(deftest ^:parallel honeysql-field-smoke-test
  (testing "->honeysql for :field produces a HoneySQL identifier without throwing"
    ;; :field impl looks up table metadata via qp.store; wrap with a mock provider so the store is initialized.
    ;; Use a real field id from the test metadata and supply ::add/source-table so the parent method can resolve
    ;; the field's table.
    ;; Note: :field clause order differs between Metabase versions:
    ;;   - master (PR #77529+):  [:field opts id-or-name]
    ;;   - v0.63.x and earlier:  [:field id-or-name opts]
    ;; Detect which order to use based on sql-mbql5 availability (removed in master).
    (let [field-id   (lib.test-md/id :venues :name)
          field-opts {:base-type         :type/Text
                      ::add/source-table (lib.test-md/id :venues)
                      ::add/source-alias "NAME"
                      :lib/uuid          "test-uuid"}
          mbql5?     (try
                       (require 'metabase.driver.sql-mbql5)
                       true
                       (catch Throwable _ false))
          field-clause (if mbql5?
                         ;; v0.63.x: [:field id-or-name opts]
                         [:field field-id field-opts]
                         ;; master: [:field opts id-or-name]
                         [:field field-opts field-id])]
      (qp.store/with-metadata-provider lib.test-md/metadata-provider
        (is (some? (sql.qp/->honeysql :maxcompute field-clause)))))))

(deftest ^:parallel honeysql-now-smoke-test
  (testing "->honeysql for :now produces a HoneySQL form without throwing"
    (is (some? (sql.qp/->honeysql :maxcompute [:now {:lib/uuid "test-uuid"}])))))

(deftest ^:parallel current-datetime-honeysql-form-test
  (testing "current-datetime-honeysql-form returns a form HoneySQL can format"
    (let [form (sql.qp/current-datetime-honeysql-form :maxcompute)]
      (is (some? form))
      (is (some? (sql/format-expr form {:nested true}))))))

;;; 2026-09-07 unix-timestamp->honeysql fix (ODPS-0130121 / ODPS-0130071 on real engine):
;;; the old impl emitted nonexistent TIMESTAMP_MILLIS/SECONDS/MICROS built-ins. New impl must
;;; produce engine-verified SQL shapes (see maxcompute.clj fix note). Rendering here is with
;;; the default :ansi dialect; the driver's quote-style (:mysql) only affects identifiers.

(defn- format-unix-ts [unit expr]
  (sql/format-expr (sql.qp/unix-timestamp->honeysql :maxcompute unit expr)
                   {:nested true :quoting :mysql}))
(deftest ^:parallel unix-timestamp->honeysql-seconds-test
  (testing ":seconds compiles to FROM_UNIXTIME with a BIGINT-cast arg inside CAST(... AS TIMESTAMP)"
    (is (= [ "CAST(FROM_UNIXTIME(CAST(t.x AS bigint)) AS timestamp)" ]
           (format-unix-ts :seconds (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))))))

(deftest ^:parallel unix-timestamp->honeysql-milliseconds-test
  (testing ":milliseconds compiles to the split-and-recombine shape with 3-digit fraction"
    (is (= [ "CAST(CONCAT(TO_CHAR(FROM_UNIXTIME(CAST(t.x / 1000 AS bigint)), 'yyyy-mm-dd hh:mi:ss'), '.', LPAD(CAST(t.x % 1000 AS STRING), 3, '0')) AS timestamp)" ]
           (format-unix-ts :milliseconds (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))))))

(deftest ^:parallel unix-timestamp->honeysql-subsecond-preserves-fraction-test
  (testing "ms and us render distinct shapes (3 vs 6 padding digits, 1000 vs 1000000 divisor)"
    (is (= [ "CAST(CONCAT(TO_CHAR(FROM_UNIXTIME(CAST(t.x / 1000 AS bigint)), 'yyyy-mm-dd hh:mi:ss'), '.', LPAD(CAST(t.x % 1000 AS STRING), 3, '0')) AS timestamp)" ]
           (format-unix-ts :milliseconds (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))))
    (is (= [ "CAST(CONCAT(TO_CHAR(FROM_UNIXTIME(CAST(t.x / 1000000 AS bigint)), 'yyyy-mm-dd hh:mi:ss'), '.', LPAD(CAST(t.x % 1000000 AS STRING), 6, '0')) AS timestamp)" ]
           (format-unix-ts :microseconds (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))))))

(deftest ^:parallel unix-timestamp->honeysql-no-regression-test
  (testing "the former broken forms are gone"
    (let [ms-str (first (format-unix-ts :milliseconds (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}])))
          s-str  (first (format-unix-ts :seconds      (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}])))]
      (testing "never emits the DOUBLE-producing FROM_UNIXTIME(x / 1000.0) style (ODPS-0130121)"
        (is (not (re-find #"FROM_UNIXTIME\(t\.x / 1000" s-str)))
        (is (not (re-find #"1000\.0" ms-str))))
      (testing "never emits nonexistent BigQuery built-ins (ODPS-0130071)"
        (is (not (str/includes? (str/upper-case ms-str) "TIMESTAMP_MILLIS")))
        (is (not (str/includes? (str/upper-case s-str) "TIMESTAMP_SECONDS")))))))

;;; 2026-09-08 engine-audit regression tests (audit rounds A–AE, live engine):
;;; every shape below was engine-verified before being asserted here.
(deftest ^:parallel trunc-renders-short-tokens-test
  (testing "DATETRUNC short token 'mi'; never 'minute'"
    (let [form (sql.qp/date :maxcompute :minute (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))
          s (first (sql/format-expr form {:nested true}))]
      (is (str/includes? s "DATETRUNC("))
      (is (str/includes? s "'mi'"))
      (is (not (str/includes? s "'minute'"))))))

(deftest ^:parallel day-of-year-composite-test
  (testing ":day-of-year via DATEDIFF composite; never EXTRACT(dayofyear)"
    (let [form (sql.qp/date :maxcompute :day-of-year (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))
          s (first (sql/format-expr form {:nested true}))]
      (is (str/includes? (str/upper-case s) "DATEDIFF("))
      (is (str/includes? s "'dd'"))
      (is (not (str/includes? (str/upper-case s) "DAYOFYEAR"))))))

(deftest ^:parallel quarter-composite-test
  (testing ":quarter month-shift composite"
    (let [form (sql.qp/date :maxcompute :quarter (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))
          q (first (sql/format-expr form {:nested true}))
          qy-form (sql.qp/date :maxcompute :quarter-of-year (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))
          qy (first (sql/format-expr qy-form {:nested true}))]
      (is (str/includes? (str/upper-case q) "DATEADD("))
      (is (str/includes? q "'mm'"))
      (is (str/includes? (str/upper-case qy) "CAST("))
      (is (not (re-find #"DATETRUNC\([^,]+, 'quarter'" (str/lower-case q)))))))

(deftest ^:parallel day-of-week-weekday-composite-test
  (testing ":day-of-week uses WEEKDAY and % operator; never EXTRACT(dayofweek) or MOD()"
    (let [form (sql.qp/date :maxcompute :day-of-week (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))
          s (first (sql/format-expr form {:nested true}))]
      (is (str/includes? s "WEEKDAY("))
      (is (str/includes? s "%"))
      (is (not (str/includes? (str/upper-case s) "MOD(")))
      (is (not (str/includes? (str/upper-case s) "DAYOFWEEK"))))))

(deftest ^:parallel week-of-year-iso-weekofyear-test
  (testing ":week-of-year-iso via WEEKOFYEAR; never isoweek"
    (let [form (sql.qp/date :maxcompute :week-of-year-iso (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}]))
          s (first (sql/format-expr form {:nested true}))]
      (is (str/includes? s "WEEKOFYEAR("))
      (is (not (str/includes? (str/upper-case s) "ISOWEEK"))))))

(deftest ^:parallel datetime-diff-native-datediff-test
  (testing "datetime-diff renders native DATEDIFF; no ghost TIMESTAMP_DIFF/DATETIME_DIFF"
    (let [diff (fn [u] (first (sql/format-expr
                               (sql.qp/datetime-diff :maxcompute u
                                                     (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}])
                                                     (sql.qp/->honeysql :maxcompute [:field "y" {::add/source-table "t" ::add/source-alias "y"}]))
                               {:nested true})))
          s-s (diff :second)
          s-h (diff :hour)
          s-m (diff :month)
          s-w (diff :week)]
      (is (str/includes? (str/upper-case s-s) "DATEDIFF("))
      (is (str/includes? s-s "'ss'"))
      (is (str/includes? s-h "'hh'"))
      (is (str/includes? s-m "'mm'"))
      (is (str/includes? (str/upper-case s-m) "CASE"))
      (is (str/includes? (str/upper-case s-w) "/ 7"))
      (is (not (str/includes? (str/upper-case s-s) "TIMESTAMP_DIFF")))
      (is (not (str/includes? (str/upper-case s-m) "DATETIME_DIFF"))))))

(deftest ^:parallel cast-temporal-string-generic-test
  (testing "generic String->Temporal coercion uses SUBSTR(1,19) cast (engine Q4/P-series probes)"
    (let [field-expr [:field "x" {::add/source-table "t" ::add/source-alias "x"}]
          compiled (sql.qp/cast-temporal-string
                     :maxcompute :Coercion/String->Temporal
                     (sql.qp/->honeysql :maxcompute field-expr))
          rendered (first (sql/format-expr compiled {:nested true}))]
      (is (str/includes? (str/upper-case rendered) "SUBSTR("))
      (is (str/includes? (str/upper-case rendered) "AS DATETIME")))))

(deftest ^:parallel misc-fixed-shapes-test
  (testing "float->DOUBLE, log base-first, YYYYMMDDHHMMSS->TO_DATE, ISO strip, GETDATE"
    (let [f-str (first (sql/format-expr (sql.qp/->float :maxcompute (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}])) {:nested true}))
          l-str (first (sql/format-expr (sql.qp/->honeysql :maxcompute [:log [:field "x" {::add/source-table "t" ::add/source-alias "x"}]]) {:nested true}))
          y-str (first (sql/format-expr (sql.qp/cast-temporal-string :maxcompute :Coercion/YYYYMMDDHHMMSSString->Temporal (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}])) {:nested true}))
          i-str (first (sql/format-expr (sql.qp/cast-temporal-string :maxcompute :Coercion/ISO8601->DateTime (sql.qp/->honeysql :maxcompute [:field "x" {::add/source-table "t" ::add/source-alias "x"}])) {:nested true}))
          g-str (first (sql/format-expr (sql.qp/current-datetime-honeysql-form :maxcompute) {:nested true}))]
      (is (str/includes? (str/upper-case f-str) "AS DOUBLE"))
      (is (not (str/includes? (str/upper-case f-str) "FLOAT64")))
      (is (str/includes? l-str "LOG(10"))
      (is (str/includes? (str/upper-case y-str) "TO_DATE("))
      (is (str/includes? y-str "yyyymmddhhmiss"))
      (is (not (str/includes? (str/upper-case y-str) "PARSE_DATETIME")))
      (is (str/includes? (str/upper-case i-str) "REPLACE("))
      (is (str/includes? i-str "'T'"))
      (is (str/includes? i-str "'Z'"))
      (is (str/includes? (str/upper-case g-str) "GETDATE("))
      (is (not (str/includes? (str/upper-case g-str) "CURRENT_TIMESTAMP"))))))

;; 2026-10-22 capability gap fills (engine-verified against live demo_project).
;; The feature flags flipped in this round are :percentile-aggregations,
;; :standard-deviation-aggregations, :advanced-math-expressions, :regex,
;; :binning, :full-join and :convert-timezone. `driver/database-supports?`
;; needs an initialized driver, which unit tests do not have, so the SQL shape
;; below is what is pinned here.
(deftest ^:parallel percentile-sql-test
  (testing "percentile/median use the native PERCENTILE(x, p), never APPROX_QUANTILES"
    (let [field [::add/source-table "t" ::add/source-alias "x"]
          rendered (fn [p]
                     (first (sql/format-expr
                             (sql.qp/->honeysql :maxcompute
                                                [:percentile [:field "x" field] p])
                             {:nested true})))
          median-str (first (sql/format-expr
                             (sql.qp/->honeysql :maxcompute [:median [:field "x" field]])
                             {:nested true}))]
      (is (re-find #"(?i)PERCENTILE\(.+0\.9\)" (rendered 0.9)))
      (is (re-find #"(?i)PERCENTILE\(.+0\.5\)" (rendered 0.5)))
      (is (re-find #"(?i)PERCENTILE\(.+0\.5\)" median-str))
      (is (not (str/includes? (str/upper-case median-str) "APPROX_QUANTILES")))
      (testing "renamed aggregation carries a clause metadata map (production shape)"
        (is (re-find #"(?i)PERCENTILE\(.+0\.9\)"
                     (first (sql/format-expr
                             (sql.qp/->honeysql :maxcompute
                                                [:percentile {:name "input_p90"
                                                              :display-name "input_p90"
                                                              :effective-type :type/Integer}
                                                 [:field "x" field] 0.9])
                             {:nested true}))))
        (is (re-find #"(?i)PERCENTILE\(.+0\.5\)"
                     (first (sql/format-expr
                             (sql.qp/->honeysql :maxcompute
                                                [:median {:name "input_median"} [:field "x" field]])
                             {:nested true}))))))))

;; 2026-10-22 production bug: cards built in the GUI that end up as an aggregate
;; over a source query rendered their inner columns as `schema`.`table`.`col`,
;; which MaxCompute cannot resolve — only `` `project.schema.table`.`col` `` works
;; (verified: `schema.table.col`, `table.col` both ODPS-0130071). The join/source-query
;; guard in `should-qualify-identifier?` suppressed the project prefix for every
;; field whose ::add/source-table was not an integer.
(deftest ^:parallel nested-query-qualification-test
  (let [mp (lib.tu/mock-metadata-provider
             {:database {:lib/type :metadata/database, :id 1 :name "test-mc" :engine :maxcompute
                         :details {:project "demo_project" :endpoint "http://x" :ak "a" :sk "b"}}
              :tables   [{:lib/type :metadata/table, :id 100 :name "users" :schema "demo"}]
              :fields   [{:lib/type :metadata/column, :id 1001 :name "user_id" :table-id 100 :base-type :type/Text}
                         {:lib/type :metadata/column, :id 1002 :name "created_at" :table-id 100 :base-type :type/BigInteger}]})
        sql-text (qp.store/with-metadata-provider mp
                   (driver/with-driver :maxcompute
                     (:query
                       (qp.compile/compile-with-inline-parameters
                         {:database 1
                          :type     :query
                          :lib/metadata mp
                          :query    {:source-query {:source-table 100
                                                    :fields [[:field 1001 nil] [:field 1002 nil]]}
                                     :aggregation [[:count]]
                                     :breakout    [[:field "user_id" {:base-type :type/Text}]]}}))))]
    (is (str/includes? sql-text "`demo_project.demo.users`.`user_id`")
        "inner columns must be qualified with <project>.<schema>.<table> as one identifier")
    (is (not (str/includes? sql-text "`demo.users`.`")))))


;; 2026-10-22 review fix — both composites below were only ever verified as
;; hand-written SQL, so the driver's own rendering drifted: `:quarter` emitted
;; the nonexistent `*-1(...)` function and backed off `(m-1)/3*3` months
;; (Oct -> Jan 1), and `:day-of-week` handed `adjust-day-of-week` a
;; Sunday-based value while `db-start-of-week` says :monday (Fri came back 7).
(deftest ^:parallel quarter-and-day-of-week-sql-test
  (let [expr (sql.qp/->honeysql :maxcompute
                                [:field "x" {::add/source-table "t" ::add/source-alias "x"}])
        q    (first (sql/format-expr (sql.qp/date :maxcompute :quarter expr) {:nested true}))
        d    (first (sql/format-expr (sql.qp/date :maxcompute :day-of-week expr) {:nested true}))
        qu   (str/upper-case q)
        du   (str/upper-case d)]
    (testing "quarter: DATEADD(DATETRUNC(x,'mm'), CAST(-1 * ((DATEPART(x,'mm')-1) % 3) AS BIGINT), 'mm')"
      (is (str/includes? qu "DATEADD("))
      (is (str/includes? qu "DATEPART("))
      (is (str/includes? q "% 3"))
      (is (str/includes? qu "AS BIGINT"))
      (is (not (str/includes? q "*-1")) "`[:*-1 ...]` renders an invalid *-1(...) call"))
    (testing "day-of-week: WEEKDAY composite (the 1..7 numbering itself is live-verified:
              Mon=1 .. Sun=7, Fri=5; adjust-day-of-week then shifts it to the
              instance start-of-week, so the rendered text is asserted loosely)"
      (is (str/includes? du "WEEKDAY("))
      (is (str/includes? du "% 7"))
      ;; WEEKDAY takes DATETIME only; a bare TIMESTAMP argument is ODPS-0130121
      (is (str/includes? du "WEEKDAY(CAST("))
      (is (str/includes? du "AS DATETIME)")))))


;; 2026-10-22 production bug: an absolute-datetime filter rendered
;; TIMESTAMP(datetime"2026-10-15 23:00:00Z") and the engine rejected it
;; (ODPS-0130161 invalid DATETIME format). `datetime"..."` never accepts a zone
;; suffix; instants must be inlined as `timestamp"<UTC wall clock>Z"`, which the
;; engine honours (23:00Z reads back as 07:00+08:00), and a time-of-day needs a
;; date anchor because `datetime"23:00:00"` is also invalid.
(deftest ^:parallel temporal-literal-inline-test
  (let [utc-plus-8 (t/offset-date-time 2026 10 16 7 0 0 0 (t/zone-offset "+08:00"))
        zdt       (t/zoned-date-time 2026 10 16 7 0 0 0 (t/zone-id "Asia/Shanghai"))
        inst      (Instant/parse "2026-10-15T23:00:00Z")
        inline    (fn [v] (sql.qp/inline-value :maxcompute v))]
    (testing "instants carry a UTC wall clock and a trailing Z"
      (is (= "timestamp\"2026-10-15 23:00:00Z\"" (inline utc-plus-8)))
      (is (= "timestamp\"2026-10-15 23:00:00Z\"" (inline zdt)))
      (is (= "timestamp\"2026-10-15 23:00:00Z\"" (inline inst))))
    (testing "no zone suffix on datetime/date literals"
      (is (= "timestamp\"2026-10-15 23:00:00\"" (inline (t/local-date-time 2026 10 15 23 0 0))))
      (is (= "date\"2026-10-15\"" (inline (t/local-date 2026 10 15))))
      (is (not (str/includes? (inline (t/local-date-time 2026 10 15 23 0 0)) "Z"))))
    (testing "time-of-day literals are anchored to a date"
      (is (= "datetime\"1970-01-01 23:00:00\"" (inline (t/local-time 23 0 0))))
      (is (= "datetime\"1970-01-01 23:00:00\"" (inline (t/offset-time 23 0 0 0 (t/zone-offset 0))))))))

;; 2026-10-22 sweep finding: core adds `ESCAPE '\'` to LIKE filters, which
;; MaxCompute rejects (ODPS-0130161 invalid token) — every :contains /
;; :starts-with / :ends-with filter failed. The engine has a built-in backslash
;; escape, so the clause must be omitted.
(deftest ^:parallel like-filter-sql-test
  (let [mp (lib.tu/mock-metadata-provider
             {:database {:lib/type :metadata/database, :id 1 :name "mc" :engine :maxcompute
                         :details {:project "demo_project" :endpoint "http://x" :ak "a" :sk "b"}}
              :tables   [{:lib/type :metadata/table, :id 100 :name "users" :schema "demo"}]
              :fields   [{:lib/type :metadata/column, :id 1001 :name "ds" :table-id 100
                          :base-type :type/Text :effective-type :type/Text}]})
        compile-filter (fn [clause]
                         (qp.store/with-metadata-provider mp
                           (driver/with-driver :maxcompute
                             (:query
                               (qp.compile/compile-with-inline-parameters
                                 {:database 1 :type :query :lib/metadata mp
                                  :query {:source-table 100
                                          :aggregation [[:count]]
                                          :filter clause}})))))]
    (testing "LIKE filters must not emit an ESCAPE clause"
      (doseq [clause [[:contains [:field 1001 nil] "2026"]
                      [:starts-with [:field 1001 nil] "2026"]
                      [:ends-with [:field 1001 nil] "16"]]]
        (let [sql-text (compile-filter clause)]
          (is (str/includes? (str/upper-case sql-text) "LIKE"))
          (is (not (str/includes? (str/upper-case sql-text) "ESCAPE"))))))))

;; 2026-10-23 deploy finding: the SQL Metabase displays/logs was formatted with
;; sql-formatter's StandardSql dialect (no :maxcompute entry in
;; metabase.driver.sql.util/dialects), which mangles backtick identifiers into
;; ` tbl `.` col ` — invalid on the engine if a user copies it out of the UI.
(deftest ^:parallel prettify-native-form-test
  (let [fmt (get-method driver/prettify-native-form :maxcompute)
        sql-in "SELECT `proj.schema.tbl`.`ds` AS `ds` FROM `proj.schema.tbl` LIMIT 3"
        out    (fmt :maxcompute sql-in)]
    (testing "backtick identifiers must not be padded with spaces"
      ;; StandardSql (the fallback when a driver has no dialect) renders
      ;; `tbl`.`col` as ` tbl `.` col `; a backtick must never be followed by
      ;; whitespace in valid MaxCompute SQL.
      (is (not (str/includes? out "` proj")))
      (testing "and identifiers must keep their exact quoting"
        (is (str/includes? out "`proj.schema.tbl`.`ds`"))
        (is (str/includes? out "`proj.schema.tbl`"))))))
