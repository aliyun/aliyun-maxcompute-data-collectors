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

package com.aliyun.odps.datacarrier.taskscheduler;

import static org.fusesource.jansi.Ansi.ansi;

import java.io.PrintStream;

import org.fusesource.jansi.Ansi;

public class InPlaceUpdates {

  public static final int MIN_TERMINAL_WIDTH = 94;

  public static boolean isUnixTerminal() {

    String os = System.getProperty("os.name");
    if (os.startsWith("Windows")) {
      // We do not support Windows, we will revisit this if we really need it for windows.
      return false;
    }

    // Invoke jansi.internal.CLibrary.isatty might cause jvm crash, so we use a safer but rouger
    // way to judge tty
    // http://stackoverflow.com/questions/1403772/how-can-i-check-if-a-java-programs-input-output-streams-are-connected-to-a-term
    return System.console() != null;
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Erases the current line and prints the given line.
   * @param line - line to print
   */
  public static void reprintLine(PrintStream out, String line) {
    out.print(ansi().eraseLine(Ansi.Erase.ALL).a(line).a('\n').toString());
    out.flush();
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Erases the current line and prints the given multiline. Make sure the specified line is not
   * terminated by linebreak.
   * @param line - line to print
   */
  public static int reprintMultiLine(PrintStream out, String line) {
    String [] lines = line.split("\r\n|\r|\n");
    int numLines = lines.length;
    for (String str : lines) {
      out.print(ansi().eraseLine(Ansi.Erase.ALL).a(str).a('\n').toString());
    }

    out.flush();
    return numLines;
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Erases the current line and prints the given line with the specified color.
   * @param line - line to print
   * @param color - color for the line
   */
  public static void reprintLineWithColorAsBold(PrintStream out, String line, Ansi.Color color) {
    out.print(ansi().eraseLine(Ansi.Erase.ALL).fg(color).bold().a(line).a('\n').boldOff().reset()
                  .toString());
    out.flush();
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Repositions the cursor back to line 0.
   */
  public static void rePositionCursor(PrintStream out, int lines) {
    out.print(ansi().cursorUp(lines).toString());
    out.flush();
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Repositions the cursor to top left, and clear screen
   */
  public static void resetScreen(PrintStream out) {
    out.print(ansi().cursor(1, 1).toString());
    out.print(ansi().eraseScreen(Ansi.Erase.FORWARD).toString());
    out.flush();
  }

  public static void resetForward(PrintStream out) {
    out.print(ansi().eraseScreen(Ansi.Erase.FORWARD).toString());
    out.flush();
  }

  /**
   * NOTE: Use this method only if isUnixTerminal is true.
   * Clear the screen, and the repositions the cursor to top left
   */
  public static void clearScreen(PrintStream out) {
    out.print(ansi().eraseScreen(Ansi.Erase.ALL).toString());
    out.print(ansi().cursor(1, 1).toString());
    out.flush();
  }
}