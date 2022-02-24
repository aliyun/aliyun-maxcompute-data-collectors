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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.cupid.table.v1.reader.filter;

import com.aliyun.odps.cupid.table.v1.util.Validator;

import java.io.Serializable;
import java.util.Arrays;

public final class FilterExpression implements Serializable {

    private final FilterType type;
    private String attribute;
    private FilterExpression[] children;
    private Object literal;

    public FilterExpression(FilterType type) {
        this.type = type;
    }

    public final FilterType getType() {
        return type;
    }

    public final String getAttribute() {
        return attribute;
    }

    public final FilterExpression[] getChildren() {
        return children;
    }

    public Object getLiteral() {
        return literal;
    }

    public static FilterExpression EqualTo(String attribute, Object value) {
        Validator.checkString(attribute, "attribute");
        FilterExpression expr = new FilterExpression(FilterType.EQUAL_TO);
        expr.attribute = attribute;
        expr.literal = value;
        return expr;
    }

    public static FilterExpression EqualNullSafe(String attribute, Object value) {
        Validator.checkString(attribute, "attribute");
        FilterExpression expr = new FilterExpression(FilterType.EQUAL_NULL_SAFE);
        expr.attribute = attribute;
        expr.literal = value;
        return expr;
    }

    public static FilterExpression GreaterThan(String attribute, Object value) {
        Validator.checkString(attribute, "attribute");
        FilterExpression expr = new FilterExpression(FilterType.GREATER_THAN);
        expr.attribute = attribute;
        expr.literal = value;
        return expr;
    }

    public static FilterExpression GreaterThanOrEqual(String attribute, Object value) {
        Validator.checkString(attribute, "attribute");
        FilterExpression expr = new FilterExpression(FilterType.GREATER_THAN_OR_EQUAL);
        expr.attribute = attribute;
        expr.literal = value;
        return expr;
    }

    public static FilterExpression LessThan(String attribute, Object value) {
        Validator.checkString(attribute, "attribute");
        FilterExpression expr = new FilterExpression(FilterType.LESS_THAN);
        expr.attribute = attribute;
        expr.literal = value;
        return expr;
    }

    public static FilterExpression LessThanOrEqual(String attribute, Object value) {
        Validator.checkString(attribute, "attribute");
        FilterExpression expr = new FilterExpression(FilterType.LESS_THAN_OR_EQUAL);
        expr.attribute = attribute;
        expr.literal = value;
        return expr;
    }

    public static FilterExpression In(String attribute, Object[] values) {
        Validator.checkString(attribute, "attribute");
        Validator.checkNotNull(values, "values");
        FilterExpression expr = new FilterExpression(FilterType.IN);
        expr.attribute = attribute;
        expr.literal = values;
        return expr;
    }

    public static FilterExpression IsNull(String attribute) {
        Validator.checkString(attribute, "attribute");
        FilterExpression expr = new FilterExpression(FilterType.IS_NULL);
        expr.attribute = attribute;
        return expr;
    }

    public static FilterExpression IsNotNull(String attribute) {
        Validator.checkString(attribute, "attribute");
        FilterExpression expr = new FilterExpression(FilterType.IS_NOT_NULL);
        expr.attribute = attribute;
        return expr;
    }

    public static FilterExpression And(FilterExpression left, FilterExpression right) {
        Validator.checkNotNull(left, "left");
        Validator.checkNotNull(right, "right");
        FilterExpression expr = new FilterExpression(FilterType.AND);
        expr.children = new FilterExpression[2];
        expr.children[0] = left;
        expr.children[1] = right;
        return expr;
    }

    public static FilterExpression And(FilterExpression[] children) {
        Validator.checkArray(children, 2, "children");
        FilterExpression expr = new FilterExpression(FilterType.AND);
        expr.children = children;
        return expr;
    }

    public static FilterExpression Or(FilterExpression left, FilterExpression right) {
        Validator.checkNotNull(left, "left");
        Validator.checkNotNull(right, "right");
        FilterExpression expr = new FilterExpression(FilterType.OR);
        expr.children = new FilterExpression[2];
        expr.children[0] = left;
        expr.children[1] = right;
        return expr;
    }

    public static FilterExpression Or(FilterExpression[] children) {
        Validator.checkArray(children, 2, "children");
        FilterExpression expr = new FilterExpression(FilterType.OR);
        expr.children = children;
        return expr;
    }

    public static FilterExpression Not(FilterExpression child) {
        Validator.checkNotNull(child, "child");
        FilterExpression expr = new FilterExpression(FilterType.NOT);
        expr.children = new FilterExpression[1];
        expr.children[0] = child;
        return expr;
    }

    public static FilterExpression StringStartsWith(String attribute, String value) {
        Validator.checkString(attribute, "attribute");
        Validator.checkString(value, "value");
        FilterExpression expr = new FilterExpression(FilterType.STRING_STARTS_WITH);
        expr.attribute = attribute;
        expr.literal = value;
        return expr;
    }

    public static FilterExpression StringEndsWith(String attribute, String value) {
        Validator.checkString(attribute, "attribute");
        Validator.checkString(value, "value");
        FilterExpression expr = new FilterExpression(FilterType.STRING_ENDS_WITH);
        expr.attribute = attribute;
        expr.literal = value;
        return expr;
    }

    public static FilterExpression StringContains(String attribute, String value) {
        Validator.checkString(attribute, "attribute");
        Validator.checkString(value, "value");
        FilterExpression expr = new FilterExpression(FilterType.STRING_CONTAINS);
        expr.attribute = attribute;
        expr.literal = value;
        return expr;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        boolean isCompound = false;
        boolean isBinary = false;

        switch (type) {
            case AND:
                isCompound = true;
                builder.append("And");
                break;
            case OR:
                isCompound = true;
                builder.append("Or");
                break;
            case NOT:
                isCompound = true;
                builder.append("Not");
                break;
            case EQUAL_TO:
                isBinary = true;
                builder.append("EqualTo");
                break;
            case EQUAL_NULL_SAFE:
                isBinary = true;
                builder.append("EqualNullSafe");
                break;
            case GREATER_THAN:
                isBinary = true;
                builder.append("GreaterThan");
                break;
            case GREATER_THAN_OR_EQUAL:
                isBinary = true;
                builder.append("GreaterThanOrEqual");
                break;
            case LESS_THAN:
                isBinary = true;
                builder.append("LessThan");
                break;
            case LESS_THAN_OR_EQUAL:
                isBinary = true;
                builder.append("LessThanOrEqual");
                break;
            case IN:
                isBinary = true;
                builder.append("In");
                break;
            case STRING_STARTS_WITH:
                isBinary = true;
                builder.append("StringStartsWith");
                break;
            case STRING_ENDS_WITH:
                isBinary = true;
                builder.append("StringEndsWith");
                break;
            case STRING_CONTAINS:
                isBinary = true;
                builder.append("StringContains");
                break;
            case IS_NULL:
                builder.append("IsNull");
                break;
            case IS_NOT_NULL:
                builder.append("IsNotNull");
                break;
        }

        if (isCompound) {
            builder.append(mkString(children));
        } else if (isBinary) {
            builder.append("(");
            builder.append(attribute);
            builder.append(", ");
            builder.append(mkString(literal));
            builder.append(")");
        } else {
            builder.append("(");
            builder.append(attribute);
            builder.append(")");
        }

        return builder.toString();
    }

    private static String mkString(FilterExpression[] filters) {
        StringBuilder builder = new StringBuilder();
        builder.append("(");
        for (int i = 0; i < filters.length; i++) {
            if (i != 0) {
                builder.append(", ");
            }
            builder.append(filters[i]);
        }
        builder.append(")");
        return builder.toString();
    }

    private static String mkString(Object literal) {
        if (literal instanceof Object[]) {
            return Arrays.toString((Object[]) literal);
        } else {
            return literal.toString();
        }
    }
}
