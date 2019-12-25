package com.aliyun.odps.datacarrier.taskscheduler;

public class ColorsGenerator {

  private static final String ANSI_RESET = "\u001B[0m";
  private static final String ANSI_BLACK = "\u001B[30m";
  private static final String ANSI_RED = "\u001B[31m";
  private static final String ANSI_GREEN = "\u001B[32m";
  private static final String ANSI_YELLOW = "\u001B[33m";
  private static final String ANSI_BLUE = "\u001B[34m";
  private static final String ANSI_PURPLE = "\u001B[35m";
  private static final String ANSI_CYAN = "\u001B[36m";
  private static final String ANSI_WHITE = "\u001B[37m";


  public static String printRed(String inputStr) {
    StringBuilder sb = new StringBuilder(ANSI_RED);
    sb.append(inputStr).append(ANSI_RESET);
    return sb.toString();
  }

  public static String printGreen(String inputStr) {
    StringBuilder sb = new StringBuilder(ANSI_GREEN);
    sb.append(inputStr).append(ANSI_RESET);
    return sb.toString();
  }

  public static String printYellow(String inputStr) {
    StringBuilder sb = new StringBuilder(ANSI_YELLOW);
    sb.append(inputStr).append(ANSI_RESET);
    return sb.toString();
  }

  public static String printWhite(String inputStr) {
    StringBuilder sb = new StringBuilder(ANSI_WHITE);
    sb.append(inputStr).append(ANSI_RESET);
    return sb.toString();
  }
}
