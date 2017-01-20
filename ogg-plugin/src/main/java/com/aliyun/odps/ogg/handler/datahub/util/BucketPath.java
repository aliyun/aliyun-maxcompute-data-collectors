/**
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
package com.aliyun.odps.ogg.handler.datahub.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import com.google.common.base.Preconditions;

public class BucketPath {

    final public static String TAG_REGEX = "\\%(\\w|\\%)|\\%\\{([\\w\\.-]+)\\}";
    final public static Pattern tagPattern = Pattern.compile(TAG_REGEX);

    public static String escapeString(String in, long timestamp, Map rowMap) {
        Matcher matcher = tagPattern.matcher(in);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String replacement = "";
            if(matcher.group(2) != null) {
                replacement = (String) rowMap.get(matcher.group(2).toLowerCase());
                if(replacement == null) {
                    replacement = "";
                }
            } else {
                Preconditions.checkState(matcher.group(1) != null
                        && matcher.group(1).length() == 1,
                    "Expected to match single character tag in string " + in);
                char c = matcher.group(1).charAt(0);
                replacement = replaceShorthand(c, timestamp);
            }

            replacement = replacement.replaceAll("\\\\", "\\\\\\\\");
            replacement = replacement.replaceAll("\\$", "\\\\\\$");

            matcher.appendReplacement(sb, replacement);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    protected static String replaceShorthand(char c, long timestamp) {

        try {
            Preconditions.checkNotNull(timestamp, "timestamp can't be null");
        } catch (NumberFormatException e) {
            throw new RuntimeException("Unable to parse timestamp.", e);
        }

        // It's a date
        String formatString = "";
        switch (c) {
            case '%':
                return "%";
            case 'a':
                formatString = "EEE";
                break;
            case 'A':
                formatString = "EEEE";
                break;
            case 'b':
                formatString = "MMM";
                break;
            case 'B':
                formatString = "MMMM";
                break;
            case 'c':
                formatString = "EEE MMM d HH:mm:ss yyyy";
                break;
            case 'd':
                formatString = "dd";
                break;
            case 'e':
                formatString = "d";
                break;
            case 'D':
                formatString = "MM/dd/yy";
                break;
            case 'H':
                formatString = "HH";
                break;
            case 'I':
                formatString = "hh";
                break;
            case 'j':
                formatString = "DDD";
                break;
            case 'k':
                formatString = "H";
                break;
            case 'l':
                formatString = "h";
                break;
            case 'm':
                formatString = "MM";
                break;
            case 'M':
                formatString = "mm";
                break;
            case 'n':
                formatString = "M";
                break;
            case 'p':
                formatString = "a";
                break;
            case 's':
                return "" + (timestamp/1000);
            case 'S':
                formatString = "ss";
                break;
            case 't':
                // This is different from unix date (which would insert a tab character
                // here)
                return "" + timestamp;
            case 'y':
                formatString = "yy";
                break;
            case 'Y':
                formatString = "yyyy";
                break;
            case 'z':
                formatString = "ZZZ";
                break;
            default:
                return "";
        }

        SimpleDateFormat format = new SimpleDateFormat(formatString);
        format.setTimeZone(TimeZone.getDefault());
        Date date = new Date(timestamp);
        return format.format(date);
    }
}

