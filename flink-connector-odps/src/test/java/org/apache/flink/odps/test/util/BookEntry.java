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

package org.apache.flink.odps.test.util;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Objects;

/**
 * TestEntry.
 */
public class BookEntry implements Serializable {
    public Integer id;
    public String title;
    public String author;
    public Double price;
    public Integer qty;
    public String date;

    public BookEntry() {}

    public BookEntry(Integer id, String title, String author, Double price, Integer qty, String date) {
        this.id = id;
        this.title = title;
        this.author = author;
        this.price = price;
        this.qty = qty;
        this.date = date;
    }

    @Override
    public String toString() {
        return "TestEntry{" +
                "id=" + id +
                ", title='" + title + '\'' +
                ", author='" + author + '\'' +
                ", price=" + price +
                ", qty=" + qty +
                '}';
    }

    public Double getPrice() {
        return price;
    }

    public Integer getId() {
        return id;
    }

    public Integer getQty() {
        return qty;
    }

    public String getAuthor() {
        return author;
    }

    public String getTitle() {
        return title;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public void setQty(Integer qty) {
        this.qty = qty;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            BookEntry that = (BookEntry)o;
            return Objects.equals(this.author, that.author)
                    && Objects.equals(this.id, that.id)
                    && Objects.equals(this.price, that.price)
                    && Objects.equals(this.qty, that.qty)
                    && Objects.equals(this.date, that.date)
                    && Objects.equals(this.title, that.title);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(this.author, this.id, this.price, this.qty, this.date, this.title);
    }

    public static final BookEntry[] TEST_DATA = {
            new BookEntry(1001, ("Java public for dummies"), ("Tan Ah Teck"), 11.11, 11, "2019-01-09"),
            new BookEntry(1002, ("More Java for dummies"), ("Tan Ah Teck"), 22.22, 22, "2019-01-08"),
            new BookEntry(1003, ("More Java for more dummies"), ("Mohammad Ali"), 33.33, 33, "2019-01-08"),
            new BookEntry(1004, ("A Cup of Java"), ("Kumar"), 44.44, 44,"2019-01-08"),
            new BookEntry(1005, ("A Teaspoon of Java"), ("Kevin Jones"), 55.55, 55,"2019-02-09"),
            new BookEntry(1006, ("A Teaspoon of Java 1.4"), ("Kevin Jones"), 66.66, 66,"2019-02-09"),
            new BookEntry(1007, ("A Teaspoon of Java 1.5"), ("Kevin Jones"), 77.77, 77,"2019-02-07"),
            new BookEntry(1008, ("A Teaspoon of Java 1.6"), ("Kevin Jones"), 88.88, 88,"2019-02-07"),
            new BookEntry(1009, ("A Teaspoon of Java 1.7"), ("Kevin Jones"), 99.99, 99,"2019-02-08"),
            new BookEntry(1010, ("A Teaspoon of Java 1.8"), ("Kevin Jones"), 100.00, 1010,"2019-02-08")
    };

    public static final BookEntry[] TEST_DATA_GROUPED_BY_DATE = {
            new BookEntry(1001, ("Java public for dummies"), ("Tan Ah Teck"), 11.11, 11, "2019-01-09"),
            new BookEntry(1002, ("More Java for dummies"), ("Tan Ah Teck"), 22.22, 22, "2019-01-09"),
            new BookEntry(1003, ("More Java for more dummies"), ("Mohammad Ali"), 33.33, 33, "2019-01-09"),
            new BookEntry(1004, ("A Cup of Java"), ("Kumar"), 44.44, 44,"2019-01-08"),
            new BookEntry(1005, ("A Teaspoon of Java"), ("Kevin Jones"), 55.55, 55,"2019-02-09"),
            new BookEntry(1006, ("A Teaspoon of Java 1.4"), ("Kevin Jones"), 66.66, 66,"2019-02-09"),
            new BookEntry(1007, ("A Teaspoon of Java 1.5"), ("Kevin Jones"), 77.77, 77,"2019-02-07"),
            new BookEntry(1008, ("A Teaspoon of Java 1.6"), ("Kevin Jones"), 88.88, 88,"2019-02-07"),
            new BookEntry(1009, ("A Teaspoon of Java 1.7"), ("Kevin Jones"), 99.99, 99,"2019-02-08"),
            new BookEntry(1010, ("A Teaspoon of Java 1.8"), ("Kevin Jones"), 100.00, 1010,"2019-02-10")
    };

    public static Row toRow(BookEntry entry) {
        Row row = new Row(6);
        row.setField(0, entry.id);
        row.setField(1, entry.title);
        row.setField(2, entry.author);
        row.setField(3, entry.price);
        row.setField(4, entry.qty);
        row.setField(5, entry.date);
        return row;
    }

    public static Row toPartitionRow(BookEntry entry) {
        Row row = new Row(6);
        row.setField(0, entry.id);
        row.setField(1, entry.title);
        row.setField(2, entry.price);
        row.setField(3, entry.qty);
        row.setField(4, entry.author);
        row.setField(5, entry.date);
        return row;
    }

    public static Tuple6 toTuple(BookEntry entry) {
        Tuple6<Integer, String, String, Double, Integer, String> tuple6 = new Tuple6<>();
        tuple6.setField(entry.id, 0);
        tuple6.setField(entry.title, 1);
        tuple6.setField(entry.author, 2);
        tuple6.setField(entry.price, 3);
        tuple6.setField(entry.qty,4);
        tuple6.setField(entry.date,5);
        return tuple6;
    }
}