package org.apache.spark.sql.odps

import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.sources.{And, Or, StringStartsWith, _}
import org.apache.spark.sql.types.{DateType, DecimalType, IntegerType, LongType, MetadataBuilder, StringType, StructField, TimestampNTZType, TimestampType}
import org.scalatest.funsuite.AnyFunSuite

import java.math.BigDecimal
import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime, ZoneId, ZonedDateTime}

class ExecutionUtilsSuite extends AnyFunSuite {

  test("convertToOdpsPredicate should return NO_PREDICATE for an empty filter sequence") {
    val result = ExecutionUtils.convertToOdpsPredicate(Seq.empty[Filter])
    assert(result.toString === "")
  }

  test("convertToOdpsPredicate should convert AlwaysTrue to NO_PREDICATE") {
    val result = ExecutionUtils.convertToOdpsPredicate(Seq(AlwaysTrue()))
    assert(result.toString === "")
  }

  test("convertToOdpsPredicate should convert AlwaysFalse to NO_PREDICATE") {
    val result = ExecutionUtils.convertToOdpsPredicate(Seq(AlwaysFalse()))
    assert(result.toString === "")
  }

  test("convertToOdpsPredicate should convert EqualTo correctly") {
    val result = ExecutionUtils.convertToOdpsPredicate(Seq(EqualTo("column1", 42)))
    assert(result.toString === "`column1` = 42")
  }

  test("convertToOdpsPredicate should convert GreaterThan correctly") {
    val result = ExecutionUtils.convertToOdpsPredicate(Seq(GreaterThan("column1", 10)))
    assert(result.toString === "`column1` > 10")
  }

  test("convertToOdpsPredicate should handle AND conditions correctly") {
    val result = ExecutionUtils.convertToOdpsPredicate(Seq(And(EqualTo("column1", 42), GreaterThan("column2", 10))))
    assert(result.toString === "`column1` = 42 and `column2` > 10")
  }

  test("convertToOdpsPredicate should handle OR conditions correctly") {
    val result = ExecutionUtils.convertToOdpsPredicate(Seq(Or(EqualTo("column1", 42), LessThan("column2", 10))))
    assert(result.toString === "(`column1` = 42 or `column2` < 10)")
  }

  test("convertToOdpsPredicate should combine predicates correctly") {
    val filters = Seq(
      EqualTo("column1", "value"),
      GreaterThan("column2", 10),
      IsNotNull("column3")
    )
    val result = ExecutionUtils.convertToOdpsPredicate(filters)
    assert(result.toString === "`column1` = 'value' and `column2` > 10 and `column3` is not null")
  }

  test("convertToOdpsPredicate should handle NOT conditions correctly") {
    val result = ExecutionUtils.convertToOdpsPredicate(Seq(Not(IsNull("column1"))))
    assert(result.toString === "(not `column1` is null)")
  }


  test("convertToOdpsPredicate should handle multiple IsNull conditions correctly") {
    val filters = Seq(IsNull("column1"), IsNull("column2"))
    val result = ExecutionUtils.convertToOdpsPredicate(filters)
    assert(result.toString === "`column1` is null and `column2` is null")
  }

  test("convertToOdpsPredicate cannot convert StringContains") {
    val result = ExecutionUtils.convertToOdpsPredicate(Seq(StringContains("column1", "subString")))
    assert(result.toString === "")
  }

  test("convertToOdpsPredicate cannot convert StringEndsWith") {
    val result = ExecutionUtils.convertToOdpsPredicate(Seq(StringEndsWith("column1", "suffix")))
    assert(result.toString === "")
  }

  test("convertToOdpsPredicate should handle deeply nested conditions correctly") {
    val filters = Seq(
      Or(
        And(EqualTo("column1", "value1"), GreaterThan("column2", 10)),
        And(EqualTo("column1", "value2"), LessThan("column3", 5)),
      )
    )
    val result = ExecutionUtils.convertToOdpsPredicate(filters)
    assert(result.toString === "((`column1` = 'value1' and `column2` > 10) or (`column1` = 'value2' and `column3` < 5))")
  }

  test("convertToOdpsPredicate should handle unknown predicate correctly (known or unknown)") {
    val filters = Seq(
      And(
        Or(EqualTo("column1", "value"), StringStartsWith("column1", "pattern")),
        And(GreaterThan("column2", 10), IsNotNull("column3"))
      ),
      StringStartsWith("column1", "pattern2"),
      Or(GreaterThan("column3", 10), IsNotNull("column4"))
    )

    val result = ExecutionUtils.convertToOdpsPredicate(filters)

    assert(result.toString === "`column2` > 10 and `column3` is not null and (`column3` > 10 or `column4` is not null)")
  }

  test("convertToOdpsPredicate should handle unknown predicate correctly (Not(known and unknown))") {
    val filters = Seq(
      Not(
        And(GreaterThan("column2", 10), StringStartsWith("column1", "str"))
      ),
      StringStartsWith("column1", "pattern2"),
      Or(GreaterThan("column3", 10), IsNotNull("column4"))
    )

    val result = ExecutionUtils.convertToOdpsPredicate(filters)

    assert(result.toString === "(`column3` > 10 or `column4` is not null)")
  }

  test("convertToOdpsPredicate should handle string correctly (escape)") {
    val filters = Seq(
      EqualTo("column2", "I'm fine.")
    )

    val result = ExecutionUtils.convertToOdpsPredicate(filters)
    println(result)

    assert(result.toString === "`column2` = 'I\\'m fine.'")
  }

  test("convertToOdpsPredicate should handle timestamp correctly (fallback to unknown)") {
    val filters = Seq(
      Not(
        And(GreaterThan("column2", 10), StringStartsWith("column1", "str"))
      ),
      StringStartsWith("column1", "pattern2"),
      Or(GreaterThan("column3", 10), IsNotNull("column4"))
    )

    val result = ExecutionUtils.convertToOdpsPredicate(filters)

    assert(result.toString === "(`column3` > 10 or `column4` is not null)")
  }

  test("quoteAttribute with quoteIfNeeded") {
    assert(ExecutionUtils.quoteAttribute(quoteIfNeeded("")) === "``")
    assert(ExecutionUtils.quoteAttribute(quoteIfNeeded("``")) === "``````")
    assert(ExecutionUtils.quoteAttribute(quoteIfNeeded("`")) === "````")
    assert(ExecutionUtils.quoteAttribute(quoteIfNeeded("ab")) === "`ab`")
    assert(ExecutionUtils.quoteAttribute(quoteIfNeeded("a b")) === "`a b`")
    assert(ExecutionUtils.quoteAttribute(quoteIfNeeded("a*b")) === "`a*b`")
    assert(ExecutionUtils.quoteAttribute(quoteIfNeeded("123")) === "`123`")
    assert(ExecutionUtils.quoteAttribute(quoteIfNeeded("1a")) === "`1a`")
    assert(ExecutionUtils.quoteAttribute(quoteIfNeeded("`1a`")) === "```1a```")
    assert(ExecutionUtils.quoteAttribute(quoteIfNeeded("`_")) === "```_`")
    assert(ExecutionUtils.quoteAttribute(quoteIfNeeded("`_`")) === "```_```")
    assert(ExecutionUtils.quoteAttribute(quoteIfNeeded("你好")) === "`你好`")
    assert(ExecutionUtils.quoteAttribute(quoteIfNeeded("你`好")) === "`你``好`")
    assert(ExecutionUtils.quoteAttribute(quoteIfNeeded("你``好")) === "`你````好`")
    assert(ExecutionUtils.quoteAttribute(quoteIfNeeded("`你好")) === "```你好`")
    assert(ExecutionUtils.quoteAttribute(quoteIfNeeded("你好`")) === "`你好```")
    assert(ExecutionUtils.quoteAttribute(quoteIfNeeded("`你好`")) === "```你好```")
  }

  test("QuoteChines") {
    val greaterThan = GreaterThan("`你好`", 2)
    val result = ExecutionUtils.convertToOdpsPredicate(Seq(greaterThan))
    println(result.toString)
    assert(result.toString == "`你好` > 2")
  }

  test("QuoteSpecialCharacter") {
    val greaterThan = GreaterThan("你`好", 2)
    val result = ExecutionUtils.convertToOdpsPredicate(Seq(greaterThan))
    println(result.toString)
    assert(result.toString == "`你``好` > 2")
  }

  test("convertToOdpsSqlPredicate should return NO_PREDICATE for an empty filter sequence") {
    val result = ExecutionUtils.createOdpsSqlFilter(Map.empty, Seq.empty[Filter])
    assert(result.getOrElse("") === "")
  }

  test("convertToOdpsSqlPredicate should convert AlwaysTrue to NO_PREDICATE") {
    val result = ExecutionUtils.createOdpsSqlFilter(Map.empty, Seq(AlwaysTrue()))
    assert(result.getOrElse("") === "")
  }

  test("convertToOdpsSqlPredicate should convert AlwaysFalse to NO_PREDICATE") {
    val result = ExecutionUtils.createOdpsSqlFilter(Map.empty, Seq(AlwaysTrue()))
    assert(result.getOrElse("") === "")
  }

  test("convertToOdpsSqlPredicate should convert EqualTo correctly") {
    val result = ExecutionUtils.createOdpsSqlFilter(Map("column1" -> StructField("column1", IntegerType))
      , Seq(EqualTo("column1", 42)))
    assert(result.getOrElse("") === "(`column1` = 42)")
  }

  test("convertToOdpsSqlPredicate should convert GreaterThan correctly") {
    val result = ExecutionUtils.createOdpsSqlFilter(Map("column1" -> StructField("column1", IntegerType))
      , Seq(GreaterThan("column1", 10)))
    assert(result.getOrElse("") === "(`column1` > 10)")
  }

  test("convertToOdpsSqlPredicate should handle AND conditions correctly") {
    val result = ExecutionUtils.createOdpsSqlFilter(
      Map("column1" -> StructField("column1", IntegerType),
        "column2" -> StructField("column2", IntegerType)),
      Seq(And(EqualTo("column1", 42), GreaterThan("column2", 10))))
    assert(result.getOrElse("") === "((`column1` = 42) AND (`column2` > 10))")
  }

  test("convertToOdpsSqlPredicate should handle OR conditions correctly") {
    val result = ExecutionUtils.createOdpsSqlFilter(
      Map("column1" -> StructField("column1", IntegerType),
        "column2" -> StructField("column2", IntegerType)),
      Seq(Or(EqualTo("column1", 42), LessThan("column2", 10))))
    assert(result.getOrElse("") === "((`column1` = 42) OR (`column2` < 10))")
  }

  test("convertToOdpsSqlPredicate should combine predicates correctly") {
    val filters = Seq(
      EqualTo("column1", "value"),
      GreaterThan("column2", 10),
      IsNotNull("column3")
    )
    val result = ExecutionUtils.createOdpsSqlFilter(
      Map("column1" -> StructField("column1", StringType),
        "column2" -> StructField("column2", IntegerType),
        "column3" -> StructField("column3", IntegerType)),
      filters)
    assert(result.getOrElse("") === "(`column1` = 'value') AND (`column2` > 10) AND (`column3` IS NOT NULL)")
  }

  test("convertToOdpsSqlPredicate should handle NOT conditions correctly") {
    val result = ExecutionUtils.createOdpsSqlFilter(
      Map("column1" -> StructField("column1", IntegerType)),
      Seq(Not(IsNull("column1"))))
    assert(result.getOrElse("") === "(NOT (`column1` IS NULL))")
  }

  test("convertToOdpsSqlPredicate should handle multiple IsNull conditions correctly") {
    val filters = Seq(IsNull("column1"), IsNull("column2"))
    val result = ExecutionUtils.createOdpsSqlFilter(
      Map("column1" -> StructField("column1", IntegerType),
        "column2" -> StructField("column2", IntegerType)),
      filters)
    assert(result.getOrElse("") === "(`column1` IS NULL) AND (`column2` IS NULL)")
  }

  test("convertToOdpsSqlPredicate convert StringContains") {
    val result = ExecutionUtils.createOdpsSqlFilter(
      Map("column1" -> StructField("column1", StringType)),
      Seq(StringContains("column1", "subString")))
    assert(result.getOrElse("") === "(`column1` LIKE '%subString%')")

    val result2 = ExecutionUtils.createOdpsSqlFilter(
      Map("column1" -> StructField("column1", StringType)),
      Seq(StringContains("column1", "b'ar")))
    assert(result2.getOrElse("") === "(`column1` LIKE '%b\\'ar%')")
  }

  test("convertToOdpsSqlPredicate convert StringEndsWith") {
    val result = ExecutionUtils.createOdpsSqlFilter(
      Map("column1" -> StructField("column1", IntegerType)),
      Seq(StringEndsWith("column1", "suffix")))
    assert(result.getOrElse("") === "(`column1` LIKE '%suffix')")

    val result2 = ExecutionUtils.createOdpsSqlFilter(
      Map("column1" -> StructField("column1", IntegerType)),
      Seq(StringEndsWith("column1", "b'ar")))
    assert(result2.getOrElse("") === "(`column1` LIKE '%b\\'ar')")
  }

  test("convertToOdpsSqlPredicate convert StringStartsWith") {
    val result = ExecutionUtils.createOdpsSqlFilter(
      Map("column1" -> StructField("column1", IntegerType)),
      Seq(StringStartsWith("column1", "prefix")))
    assert(result.getOrElse("") === "(`column1` LIKE 'prefix%')")

    val result2 = ExecutionUtils.createOdpsSqlFilter(
      Map("column1" -> StructField("column1", IntegerType)),
      Seq(StringStartsWith("column1", "b'ar")))
    assert(result2.getOrElse("") === "(`column1` LIKE 'b\\'ar%')")
  }

  test("convertToOdpsSqlPredicate should handle deeply nested conditions correctly") {
    val filters = Seq(
      Or(
        And(EqualTo("column1", "value1"), GreaterThan("column2", 10)),
        And(EqualTo("column1", "value2"), LessThan("column3", 5)),
      )
    )
    val result = ExecutionUtils.createOdpsSqlFilter(
      Map("column1" -> StructField("column1", StringType),
        "column2" -> StructField("column2", IntegerType),
        "column3" -> StructField("column3", IntegerType)),
      filters)
    assert(result.getOrElse("") === "(((`column1` = 'value1') AND (`column2` > 10)) OR ((`column1` = 'value2') AND (`column3` < 5)))")

    val filters2 = Seq(
      And(
        Or(EqualTo("column1", "value"), StringStartsWith("column1", "pattern")),
        And(GreaterThan("column2", 10), IsNotNull("column3"))
      ),
      StringStartsWith("column1", "pattern2"),
      Or(GreaterThan("column3", 10), IsNotNull("column4"))
    )

    val result2 = ExecutionUtils.createOdpsSqlFilter(
      Map("column1" -> StructField("column1", StringType),
        "column2" -> StructField("column2", IntegerType),
        "column3" -> StructField("column3", IntegerType),
        "column4" -> StructField("column4", IntegerType)),
      filters2)
    assert(result2.getOrElse("") === "(((`column1` = 'value') OR (`column1` LIKE 'pattern%')) AND ((`column2` > 10) AND (`column3` IS NOT NULL))) AND (`column1` LIKE 'pattern2%') AND ((`column3` > 10) OR (`column4` IS NOT NULL))")

    val filters3 = Seq(
      Not(
        And(GreaterThan("column2", 10), StringStartsWith("column1", "str"))
      ),
      StringStartsWith("column1", "pattern2"),
      Or(GreaterThan("column3", 10), IsNotNull("column4"))
    )

    val result3 = ExecutionUtils.createOdpsSqlFilter(
      Map("column1" -> StructField("column1", StringType),
        "column2" -> StructField("column2", IntegerType),
        "column3" -> StructField("column3", IntegerType),
        "column4" -> StructField("column4", IntegerType)),
      filters3)
    assert(result3.getOrElse("") === "(NOT ((`column2` > 10) AND (`column1` LIKE 'str%'))) AND (`column1` LIKE 'pattern2%') AND ((`column3` > 10) OR (`column4` IS NOT NULL))")
  }

  test("convertToOdpsSqlPredicate should handle decimal correctly") {
    val filters = Seq(
      In.apply("decimalfield", Array(
          new BigDecimal("1.1"),
          new BigDecimal("1.1"),
          new BigDecimal("1.2"),
          new BigDecimal("1"),
          new BigDecimal("1"),
          new BigDecimal("12345678901234567890123456789012345678"),
          new BigDecimal("99999999999999999999999999999999999999"),
          new BigDecimal("0.123456789012345678"),
          new BigDecimal("0.000000000000000001"),
          new BigDecimal("99999999999999999999.999999999999999999"),
          new BigDecimal("10000000000000000000.000000000000000001"),
          new BigDecimal("1.1")
        )
      )
    )
    val result = ExecutionUtils.createOdpsSqlFilter(
      Map("decimalfield" -> StructField("column1", DecimalType.apply(38,18))), filters)
    assert(result.getOrElse("") === "(`decimalfield` IN (1.1,1.1,1.2,1,1,12345678901234567890123456789012345678,99999999999999999999999999999999999999,0.123456789012345678,0.000000000000000001,99999999999999999999.999999999999999999,10000000000000000000.000000000000000001,1.1))")
  }

  test("convertToOdpsSqlPredicate should handle date correctly") {
    val filters = Seq(
      In.apply("datefield", Array(
        LocalDate.of(0, 1, 1),
        LocalDate.of(0, 1, 1),
        LocalDate.of(1, 1, 1),
        LocalDate.of(1900, 1, 1),
        LocalDate.of(2022, 1, 1),
        LocalDate.of(9999, 12, 31)))
    )
    val result = ExecutionUtils.createOdpsSqlFilter(
      Map("datefield" -> StructField("column1", DateType)), filters)
    assert(result.getOrElse("") === "(`datefield` IN (DATE '0000-01-01',DATE '0000-01-01',DATE '0001-01-01',DATE '1900-01-01',DATE '2022-01-01',DATE '9999-12-31'))")

    val filters2 = Seq(
      In.apply("datefield", Array(
        java.sql.Date.valueOf("0000-01-01"),
        java.sql.Date.valueOf("0001-01-01"),
        java.sql.Date.valueOf("1900-01-01"),
        java.sql.Date.valueOf("2022-01-01"),
        java.sql.Date.valueOf("9999-12-31")))
    )
    val result2 = ExecutionUtils.createOdpsSqlFilter(
      Map("datefield" -> StructField("column1", DateType)), filters2)
    // Note: java.sql.Date not equal
    assert(result2.getOrElse("") === "(`datefield` IN (DATE '0001-01-01',DATE '0001-01-01',DATE '1900-01-01',DATE '2022-01-01',DATE '9999-12-31'))")
  }

  test("convertToOdpsSqlPredicate should handle timestamp correctly") {
    val filters = Seq(
      In.apply("tsfield", Array(
        ZonedDateTime.of(0, 1, 1, 0, 0, 0, 0, ZoneId.systemDefault).toInstant,
        ZonedDateTime.of(0, 1, 1, 0, 0, 0, 123000000, ZoneId.systemDefault).toInstant,
        ZonedDateTime.of(0, 1, 1, 0, 0, 0, 123456789, ZoneId.systemDefault).toInstant,
        ZonedDateTime.of(1900, 1, 1, 8, 0, 0, 123456789, ZoneId.systemDefault).toInstant
      ))
    )
    val result = ExecutionUtils.createOdpsSqlFilter(
      Map("tsfield" -> StructField("column1", TimestampType)), filters)
    assert(result.getOrElse("") === "(`tsfield` IN (TIMESTAMP '0000-01-01 00:00:00',TIMESTAMP '0000-01-01 00:00:00.123',TIMESTAMP '0000-01-01 00:00:00.123456789',TIMESTAMP '1900-01-01 08:00:00.123456789'))")

    val filters2 = Seq(
      In.apply("tsfield", Array(
        Timestamp.valueOf("2008-12-25 15:30:00"),
        Timestamp.valueOf("2020-01-25 02:10:10")
      ))
    )
    // Note: not equal
    // Timestamp.valueOf("0000-01-01 00:00:00"),
    // Timestamp.valueOf("0000-01-01 00:00:00.123"),
    // Timestamp.valueOf("0000-01-01 00:00:00.123456789"),
    // Timestamp.valueOf("1900-01-01 08:00:00.123456789"),
    val result2 = ExecutionUtils.createOdpsSqlFilter(
      Map("tsfield" -> StructField("column1", TimestampType)), filters2)
    assert(result2.getOrElse("") === "(`tsfield` IN (TIMESTAMP '2008-12-25 15:30:00',TIMESTAMP '2020-01-25 02:10:10'))")
  }

  test("convertToOdpsSqlPredicate should handle datetime correctly") {
    val filters = Seq(
      In.apply("tsfield", Array(
        ZonedDateTime.of(0, 1, 1, 0, 0, 0, 0, ZoneId.systemDefault).toInstant,
        ZonedDateTime.of(0, 1, 1, 0, 0, 0, 123000000, ZoneId.systemDefault).toInstant,
        ZonedDateTime.of(0, 1, 1, 0, 0, 0, 123456789, ZoneId.systemDefault).toInstant,
        ZonedDateTime.of(1900, 1, 1, 8, 0, 0, 123456789, ZoneId.systemDefault).toInstant
      ))
    )
    val result = ExecutionUtils.createOdpsSqlFilter(
      Map("tsfield" -> StructField("column1", TimestampType, metadata = new MetadataBuilder()
        .putBoolean(OdpsUtils.DATETIME_TYPE_STRING_METADATA_KEY, value = true).build())), filters)

    assert(result.getOrElse("") === "(`tsfield` IN (DATETIME '0000-01-01 00:00:00',DATETIME '0000-01-01 00:00:00',DATETIME '0000-01-01 00:00:00',DATETIME '1900-01-01 08:00:00'))")

    val filters2 = Seq(
      In.apply("tsfield", Array(
        Timestamp.valueOf("2008-12-25 15:30:00"),
        Timestamp.valueOf("2020-01-25 02:10:10")
      ))
    )
    // Note: not equal
    // Timestamp.valueOf("0000-01-01 00:00:00"),
    // Timestamp.valueOf("0000-01-01 00:00:00.123"),
    // Timestamp.valueOf("0000-01-01 00:00:00.123456789"),
    // Timestamp.valueOf("1900-01-01 08:00:00.123456789"),
    val result2 = ExecutionUtils.createOdpsSqlFilter(
      Map("tsfield" -> StructField("column1", TimestampType, metadata = new MetadataBuilder()
        .putBoolean(OdpsUtils.DATETIME_TYPE_STRING_METADATA_KEY, value = true).build())), filters2)
    assert(result2.getOrElse("") === "(`tsfield` IN (DATETIME '2008-12-25 15:30:00',DATETIME '2020-01-25 02:10:10'))")
  }

  test("convertToOdpsSqlPredicate should handle timestamp_ntz correctly") {
    val filters = Seq(
      In.apply("tsfield", Array(
        LocalDateTime.of(0, 1, 1, 0, 0, 0, 0),
        LocalDateTime.of(0, 1, 1, 0, 0, 0, 123000000),
        LocalDateTime.of(0, 1, 1, 0, 0, 0, 123123123),
        LocalDateTime.of(1900, 1, 1, 8, 0, 0, 123123123),
        LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999999)
      ))
    )
    val result = ExecutionUtils.createOdpsSqlFilter(
      Map("tsfield" -> StructField("column1", TimestampNTZType)), filters)
    assert(result.getOrElse("") === "(`tsfield` IN (TIMESTAMP_NTZ '0000-01-01 00:00:00',TIMESTAMP_NTZ '0000-01-01 00:00:00.123',TIMESTAMP_NTZ '0000-01-01 00:00:00.123123123',TIMESTAMP_NTZ '1900-01-01 08:00:00.123123123',TIMESTAMP_NTZ '9999-12-31 23:59:59.999999999'))")
  }

  test("testFiltersWithNested") {
    // original query
    // (c1 >= 500 or c1 <= 70 or c1 >= 900 or c3 <= 50) and
    // (c1 >= 100 or c1 <= 700  or c2 <= 900) and
    // (c1 >= 5000 or c1 <= 701)

    val filters = Seq(
      Or.apply(Or.apply(GreaterThanOrEqual.apply("c1", 500), LessThanOrEqual.apply("c1", 70)), Or.apply(GreaterThanOrEqual.apply("c1", 900), LessThanOrEqual.apply("c3", 50))),
      Or.apply(Or.apply(GreaterThanOrEqual.apply("c1", 100), LessThanOrEqual.apply("c1", 700)), LessThanOrEqual.apply("c2", 900)),
      Or.apply(GreaterThanOrEqual.apply("c1", 5000), LessThanOrEqual.apply("c1", 701))
    )
    val result = ExecutionUtils.createOdpsSqlFilter(
      Map("c1" -> StructField("c1", LongType),
        "c2" -> StructField("c2", LongType),
        "c3" -> StructField("c3", LongType)), filters)
    assert(result.getOrElse("") === "(((`c1` >= 500) OR (`c1` <= 70)) OR ((`c1` >= 900) OR (`c3` <= 50))) AND (((`c1` >= 100) OR (`c1` <= 700)) OR (`c2` <= 900)) AND ((`c1` >= 5000) OR (`c1` <= 701))")

    // original query
    // (((c1 >= 500 or c1 <= 70) and
    // (c1 >= 900 or (c3 <= 50 and (c2 >= 20 or c3 > 200))))) and
    // (((c1 >= 5000 or c1 <= 701) and (c2 >= 150 or c3 >= 100)) or
    // ((c1 >= 50 or c1 <= 71) and (c2 >= 15 or c3 >= 10)))

    val filters2 = Seq(
      Or.apply(GreaterThanOrEqual.apply("c1", 500),
        LessThanOrEqual.apply("c1", 70)),
      Or.apply(GreaterThanOrEqual.apply("c1", 900),
        And.apply(LessThanOrEqual.apply("c3", 50),
          Or.apply(GreaterThanOrEqual.apply("c2", 20),
            GreaterThan.apply("c3", 200)))),
      Or.apply(And.apply(Or.apply(GreaterThanOrEqual.apply("c1", 5000),
        LessThanOrEqual.apply("c1", 701)),
        Or.apply(GreaterThanOrEqual.apply("c2", 150),
          GreaterThanOrEqual.apply("c3", 100))),
        And.apply(Or.apply(GreaterThanOrEqual.apply("c1", 50),
          LessThanOrEqual.apply("c1", 71)),
          Or.apply(GreaterThanOrEqual.apply("c2", 15),
            GreaterThanOrEqual.apply("c3", 10))))
    )
    val result2 = ExecutionUtils.createOdpsSqlFilter(
      Map("c1" -> StructField("c1", LongType),
        "c2" -> StructField("c2", LongType),
        "c3" -> StructField("c3", LongType)), filters2)
    assert(result2.getOrElse("") === "((`c1` >= 500) OR (`c1` <= 70)) AND " +
      "((`c1` >= 900) OR ((`c3` <= 50) AND ((`c2` >= 20) OR (`c3` > 200)))) AND " +
      "((((`c1` >= 5000) OR (`c1` <= 701)) AND ((`c2` >= 150) OR (`c3` >= 100))) OR (((`c1` >= 50) OR (`c1` <= 71)) AND ((`c2` >= 15) OR (`c3` >= 10))))"
    )

    val filters3 = Seq(
      Or.apply(Not.apply(EqualNullSafe.apply("c1", 500)), Not.apply(EqualNullSafe.apply("c2", 500))),
      EqualNullSafe.apply("c1", 500),
      EqualNullSafe.apply("c2", 500)
    )
    val result3 = ExecutionUtils.createOdpsSqlFilter(
      Map("c1" -> StructField("c1", LongType),
        "c2" -> StructField("c2", LongType)), filters3)
    assert(result3.getOrElse("") === "((NOT ((`c1` IS NULL AND 500 IS NULL) OR (`c1` IS NOT NULL AND 500 IS NOT NULL AND `c1` = 500))) " +
      "OR (NOT ((`c2` IS NULL AND 500 IS NULL) OR (`c2` IS NOT NULL AND 500 IS NOT NULL AND `c2` = 500)))) " +
      "AND ((`c1` IS NULL AND 500 IS NULL) OR (`c1` IS NOT NULL AND 500 IS NOT NULL AND `c1` = 500)) " +
      "AND ((`c2` IS NULL AND 500 IS NULL) OR (`c2` IS NOT NULL AND 500 IS NOT NULL AND `c2` = 500))"
    )
  }
}
