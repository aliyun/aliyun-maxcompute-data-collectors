package com.aliyun.odps.datacarrier.network;

import java.util.ArrayList;
import java.util.List;

/**
 * See: https://help.aliyun.com/document_detail/34951.html?spm=a2c4g.11186623.2.8.48212187QROQp6#concept-m2j-h1y-5db
 */
public class Endpoints {

  public enum LOCATION {
    /**
     * Hangzhou
     */
    HANGZHOU,
    /**
     * Shanghai
     */
    SHANGHAI,
    /**
     * Beijing
     */
    BEIJING,
    /**
     * Shenzhen
     */
    SHENZHEN,
    /**
     * Hong kong
     */
    HONG_KONG,
    /**
     * Singapore
     */
    SINGAPORE,
    /**
     * Sydney
     */
    SYDNEY,
    /**
     * Kuala Lumpur
     */
    KUALA_LUMPUR,
    /**
     * Jakarta
     */
    JAKARTA,
    /**
     * Tokyo
     */
    TOKYO,
    /**
     * Frankfurt
     */
    FRANKFURT,
    /**
     * Silicon Valley
     */
    SILICON_VALLEY,
    /**
     * Virginia
     */
    VIRGINIA,
    /**
     * Mumbai
     */
    MUMBAI,
    /**
     * Dubai
     */
    DUBAI,
    /**
     * London
     */
    LONDON
  }

  public enum NETWORK {
    /**
     * Internet
     */
    EXTERNAL,
    /**
     * VPC
     */
    VPC,
    /**
     * Classic network
     */
    CLASSIC_NETWORK
  }

  /**
   * External endpoints
   */
  public static final Endpoint EXTERNAL_CHINA_EAST_1 = new Endpoint(
      "http://service.cn.maxcompute.aliyun.com/api",
      "http://dt.cn-hangzhou.maxcompute.aliyun.com",
      LOCATION.HANGZHOU, NETWORK.EXTERNAL);
  public static final Endpoint EXTERNAL_CHINA_EAST_2 = new Endpoint(
      "http://service.cn.maxcompute.aliyun.com/api",
      "http://dt.cn-shanghai.maxcompute.aliyun.com",
      LOCATION.SHANGHAI, NETWORK.EXTERNAL);
  public static final Endpoint EXTERNAL_CHINA_NORTH_2 = new Endpoint(
      "http://service.cn.maxcompute.aliyun.com/api",
      "http://dt.cn-beijing.maxcompute.aliyun.com",
      LOCATION.BEIJING, NETWORK.EXTERNAL);
  public static final Endpoint EXTERNAL_CHINA_SOUTH_1 = new Endpoint(
      "http://service.cn.maxcompute.aliyun.com/api",
      "http://dt.cn-shenzhen.maxcompute.aliyun.com",
      LOCATION.SHENZHEN, NETWORK.EXTERNAL);
  public static final Endpoint EXTERNAL_HONG_KONG = new Endpoint(
      "http://service.cn-hongkong.maxcompute.aliyun.com/api",
      "http://dt.cn-hongkong.maxcompute.aliyun.com",
      LOCATION.HONG_KONG, NETWORK.EXTERNAL);
  public static final Endpoint EXTERNAL_ASIA_PACIFIC_SE_1 = new Endpoint(
      "http://service.ap-southeast-1.maxcompute.aliyun.com/api",
      "http://dt.ap-southeast-1.maxcompute.aliyun.com",
      LOCATION.SINGAPORE, NETWORK.EXTERNAL);
  public static final Endpoint EXTERNAL_ASIA_PACIFIC_SE_2 = new Endpoint(
      "http://service.ap-southeast-2.maxcompute.aliyun.com/api",
      "http://dt.ap-southeast-2.maxcompute.aliyun.com",
      LOCATION.SYDNEY, NETWORK.EXTERNAL);
  public static final Endpoint EXTERNAL_ASIA_PACIFIC_SE_3 = new Endpoint(
      "http://service.ap-southeast-3.maxcompute.aliyun.com/api",
      "http://dt.ap-southeast-3.maxcompute.aliyun.com",
      LOCATION.KUALA_LUMPUR, NETWORK.EXTERNAL);
  public static final Endpoint EXTERNAL_ASIA_PACIFIC_SE_5 = new Endpoint(
      "http://service.ap-southeast-5.maxcompute.aliyun.com/api",
      "http://dt.ap-southeast-5.maxcompute.aliyun.com",
      LOCATION.JAKARTA, NETWORK.EXTERNAL);
  public static final Endpoint EXTERNAL_ASIA_PACIFIC_NE_1 = new Endpoint(
      "http://service.ap-northeast-1.maxcompute.aliyun.com/api",
      "http://dt.ap-northeast-1.maxcompute.aliyun.com",
      LOCATION.TOKYO, NETWORK.EXTERNAL);
  public static final Endpoint EXTERNAL_CENTRAL_EUROPE_1 = new Endpoint(
      "http://service.eu-central-1.maxcompute.aliyun.com/api",
      "http://dt.eu-central-1.maxcompute.aliyun.com",
      LOCATION.FRANKFURT, NETWORK.EXTERNAL);
  public static final Endpoint EXTERNAL_WEST_USA_1 = new Endpoint(
      "http://service.us-west-1.maxcompute.aliyun.com/api",
      "http://dt.us-west-1.maxcompute.aliyun.com",
      LOCATION.SILICON_VALLEY, NETWORK.EXTERNAL);
  public static final Endpoint EXTERNAL_EAST_USA_1 = new Endpoint(
      "http://service.us-east-1.maxcompute.aliyun.com/api",
      "http://dt.us-east-1.maxcompute.aliyun.com",
      LOCATION.VIRGINIA, NETWORK.EXTERNAL);
  public static final Endpoint EXTERNAL_ASIA_PACIFIC_SOU_1 = new Endpoint(
      "http://service.ap-south-1.maxcompute.aliyun.com/api",
      "http://dt.ap-south-1.maxcompute.aliyun.com",
      LOCATION.MUMBAI, NETWORK.EXTERNAL);
  public static final Endpoint EXTERNAL_MIDDLE_EAST_1 = new Endpoint(
      "http://service.me-east-1.maxcompute.aliyun.com/api",
      "http://dt.me-east-1.maxcompute.aliyun.com",
      LOCATION.DUBAI, NETWORK.EXTERNAL);
  public static final Endpoint EXTERNAL_UK = new Endpoint(
      "http://service.eu-west-1.maxcompute.aliyun.com/api",
      "http://dt.eu-west-1.maxcompute.aliyun.com",
      LOCATION.LONDON, NETWORK.EXTERNAL);

  /**
   * Classic network endpoints
   */
  public static final Endpoint CLASSIC_CHINA_EAST_1 = new Endpoint(
      "http://service.cn.maxcompute.aliyun-inc.com/api",
      "http://dt.cn-hangzhou.maxcompute.aliyun-inc.com",
      LOCATION.HANGZHOU, NETWORK.CLASSIC_NETWORK);
  public static final Endpoint CLASSIC_CHINA_EAST_2 = new Endpoint(
      "http://service.cn.maxcompute.aliyun-inc.com/api",
      "http://dt.cn-shanghai.maxcompute.aliyun-inc.com",
      LOCATION.SHANGHAI, NETWORK.CLASSIC_NETWORK);
  public static final Endpoint CLASSIC_CHINA_NORTH_2 = new Endpoint(
      "http://service.cn.maxcompute.aliyun-inc.com/api",
      "http://dt.cn-beijing.maxcompute.aliyun-inc.com",
      LOCATION.BEIJING, NETWORK.CLASSIC_NETWORK);
  public static final Endpoint CLASSIC_CHINA_SOUTH_1 = new Endpoint(
      "http://service.cn.maxcompute.aliyun-inc.com/api",
      "http://dt.cn-shenzhen.maxcompute.aliyun-inc.com",
      LOCATION.SHENZHEN, NETWORK.CLASSIC_NETWORK);
  public static final Endpoint CLASSIC_HONG_KONG = new Endpoint(
      "http://service.cn-hongkong.maxcompute.aliyun-inc.com/api",
      "http://dt.cn-hongkong.maxcompute.aliyun-inc.com",
      LOCATION.HONG_KONG, NETWORK.CLASSIC_NETWORK);
  public static final Endpoint CLASSIC_ASIA_PACIFIC_SE_1 = new Endpoint(
      "http://service.ap-southeast-1.maxcompute.aliyun-inc.com/api",
      "http://dt.ap-southeast-1.maxcompute.aliyun-inc.com",
      LOCATION.SINGAPORE, NETWORK.CLASSIC_NETWORK);
  public static final Endpoint CLASSIC_ASIA_PACIFIC_SE_2 = new Endpoint(
      "http://service.ap-southeast-2.maxcompute.aliyun-inc.com/api",
      "http://dt.ap-southeast-2.maxcompute.aliyun-inc.com",
      LOCATION.SYDNEY, NETWORK.CLASSIC_NETWORK);
  public static final Endpoint CLASSIC_ASIA_PACIFIC_SE_3 = new Endpoint(
      "http://service.ap-southeast-3.maxcompute.aliyun-inc.com/api",
      "http://dt.ap-southeast-3.maxcompute.aliyun-inc.com",
      LOCATION.KUALA_LUMPUR, NETWORK.CLASSIC_NETWORK);
  public static final Endpoint CLASSIC_ASIA_PACIFIC_SE_5 = new Endpoint(
      "http://service.ap-southeast-5.maxcompute.aliyun-inc.com/api",
      "http://dt.ap-southeast-5.maxcompute.aliyun-inc.com",
      LOCATION.JAKARTA, NETWORK.CLASSIC_NETWORK);
  public static final Endpoint CLASSIC_ASIA_PACIFIC_NE_1 = new Endpoint(
      "http://service.ap-northeast-1.maxcompute.aliyun-inc.com/api",
      "http://dt.ap-northeast-1.maxcompute.aliyun-inc.com",
      LOCATION.TOKYO, NETWORK.CLASSIC_NETWORK);
  public static final Endpoint CLASSIC_CENTRAL_EUROPE_1 = new Endpoint(
      "http://service.eu-central-1.maxcompute.aliyun-inc.com/api",
      "http://dt.eu-central-1.maxcompute.aliyun-inc.com",
      LOCATION.FRANKFURT, NETWORK.CLASSIC_NETWORK);
  public static final Endpoint CLASSIC_WEST_USA_1 = new Endpoint(
      "http://service.us-west-1.maxcompute.aliyun-inc.com/api",
      "http://dt.us-west-1.maxcompute.aliyun-inc.com",
      LOCATION.SILICON_VALLEY, NETWORK.CLASSIC_NETWORK);
  public static final Endpoint CLASSIC_EAST_USA_1 = new Endpoint(
      "http://service.us-east-1.maxcompute.aliyun-inc.com/api",
      "http://dt.us-east-1.maxcompute.aliyun-inc.com",
      LOCATION.VIRGINIA, NETWORK.CLASSIC_NETWORK);
  public static final Endpoint CLASSIC_ASIA_PACIFIC_SOU_1 = new Endpoint(
      "http://service.ap-south-1.maxcompute.aliyun-inc.com/api",
      "http://dt.ap-south-1.maxcompute.aliyun-inc.com",
      LOCATION.MUMBAI, NETWORK.CLASSIC_NETWORK);
  public static final Endpoint CLASSIC_MIDDLE_EAST_1 = new Endpoint(
      "http://service.me-east-1.maxcompute.aliyun-inc.com/api",
      "http://dt.me-east-1.maxcompute.aliyun-inc.com",
      LOCATION.DUBAI, NETWORK.CLASSIC_NETWORK);
  public static final Endpoint CLASSIC_UK = new Endpoint(
      "http://service.uk-all.maxcompute.aliyun-inc.com/api",
      "http://dt.uk-all.maxcompute.aliyun-inc.com",
      LOCATION.LONDON, NETWORK.CLASSIC_NETWORK);

  /**
   * VPC endpoints
   */
  public static final Endpoint VPC_CHINA_EAST_1 = new Endpoint(
      "http://service.cn.maxcompute.aliyun-inc.com/api",
      "http://dt.cn-hangzhou.maxcompute.aliyun-inc.com",
      LOCATION.HANGZHOU, NETWORK.VPC);
  public static final Endpoint VPC_CHINA_EAST_2 = new Endpoint(
      "http://service.cn.maxcompute.aliyun-inc.com/api",
      "http://dt.cn-shanghai.maxcompute.aliyun-inc.com",
      LOCATION.SHANGHAI, NETWORK.VPC);
  public static final Endpoint VPC_CHINA_NORTH_2 = new Endpoint(
      "http://service.cn.maxcompute.aliyun-inc.com/api",
      "http://dt.cn-beijing.maxcompute.aliyun-inc.com",
      LOCATION.BEIJING, NETWORK.VPC);
  public static final Endpoint VPC_CHINA_SOUTH_1 = new Endpoint(
      "http://service.cn.maxcompute.aliyun-inc.com/api",
      "http://dt.cn-shenzhen.maxcompute.aliyun-inc.com",
      LOCATION.SHENZHEN, NETWORK.VPC);
  public static final Endpoint VPC_HONG_KONG = new Endpoint(
      "http://service.cn-hongkong.maxcompute.aliyun-inc.com/api",
      "http://dt.cn-hongkong.maxcompute.aliyun-inc.com",
      LOCATION.HONG_KONG, NETWORK.VPC);
  public static final Endpoint VPC_ASIA_PACIFIC_SE_1 = new Endpoint(
      "http://service.ap-southeast-1.maxcompute.aliyun-inc.com/api",
      "http://dt.ap-southeast-1.maxcompute.aliyun-inc.com",
      LOCATION.SINGAPORE, NETWORK.VPC);
  public static final Endpoint VPC_ASIA_PACIFIC_SE_2 = new Endpoint(
      "http://service.ap-southeast-2.maxcompute.aliyun-inc.com/api",
      "http://dt.ap-southeast-2.maxcompute.aliyun-inc.com",
      LOCATION.SYDNEY, NETWORK.VPC);
  public static final Endpoint VPC_ASIA_PACIFIC_SE_3 = new Endpoint(
      "http://service.ap-southeast-3.maxcompute.aliyun-inc.com/api",
      "http://dt.ap-southeast-3.maxcompute.aliyun-inc.com",
      LOCATION.KUALA_LUMPUR, NETWORK.VPC);
  public static final Endpoint VPC_ASIA_PACIFIC_SE_5 = new Endpoint(
      "http://service.ap-southeast-5.maxcompute.aliyun-inc.com/api",
      "http://dt.ap-southeast-5.maxcompute.aliyun-inc.com",
      LOCATION.JAKARTA, NETWORK.VPC);
  public static final Endpoint VPC_ASIA_PACIFIC_NE_1 = new Endpoint(
      "http://service.ap-northeast-1.maxcompute.aliyun-inc.com/api",
      "http://dt.ap-northeast-1.maxcompute.aliyun-inc.com",
      LOCATION.TOKYO, NETWORK.VPC);
  public static final Endpoint VPC_CENTRAL_EUROPE_1 = new Endpoint(
      "http://service.eu-central-1.maxcompute.aliyun-inc.com/api",
      "http://dt.eu-central-1.maxcompute.aliyun-inc.com",
      LOCATION.FRANKFURT, NETWORK.VPC);
  public static final Endpoint VPC_WEST_USA_1 = new Endpoint(
      "http://service.us-west-1.maxcompute.aliyun-inc.com/api",
      "http://dt.us-west-1.maxcompute.aliyun-inc.com",
      LOCATION.SILICON_VALLEY, NETWORK.VPC);
  public static final Endpoint VPC_EAST_USA_1 = new Endpoint(
      "http://service.us-east-1.maxcompute.aliyun-inc.com/api",
      "http://dt.us-east-1.maxcompute.aliyun-inc.com",
      LOCATION.VIRGINIA, NETWORK.VPC);
  public static final Endpoint VPC_ASIA_PACIFIC_SOU_1 = new Endpoint(
      "http://service.ap-south-1.maxcompute.aliyun-inc.com/api",
      "http://dt.ap-south-1.maxcompute.aliyun-inc.com",
      LOCATION.MUMBAI, NETWORK.VPC);
  public static final Endpoint VPC_MIDDLE_EAST_1 = new Endpoint(
      "http://service.me-east-1.maxcompute.aliyun-inc.com/api",
      "http://dt.me-east-1.maxcompute.aliyun-inc.com",
      LOCATION.DUBAI, NETWORK.VPC);
  public static final Endpoint VPC_UK = new Endpoint(
      "http://service.uk-all.maxcompute.aliyun-inc.com/api",
      "http://dt.uk-all.maxcompute.aliyun-inc.com",
      LOCATION.LONDON, NETWORK.VPC);

  public static List<Endpoint> getExternalEndpoints() {
    List<Endpoint> endpoints = new ArrayList<>();
    endpoints.add(EXTERNAL_CHINA_EAST_1);
    endpoints.add(EXTERNAL_CHINA_EAST_2);
    endpoints.add(EXTERNAL_CHINA_NORTH_2);
    endpoints.add(EXTERNAL_CHINA_SOUTH_1);
    endpoints.add(EXTERNAL_HONG_KONG);
    endpoints.add(EXTERNAL_ASIA_PACIFIC_SE_1);
    endpoints.add(EXTERNAL_ASIA_PACIFIC_SE_2);
    endpoints.add(EXTERNAL_ASIA_PACIFIC_SE_3);
    endpoints.add(EXTERNAL_ASIA_PACIFIC_SE_5);
    endpoints.add(EXTERNAL_ASIA_PACIFIC_NE_1);
    endpoints.add(EXTERNAL_CENTRAL_EUROPE_1);
    endpoints.add(EXTERNAL_WEST_USA_1);
    endpoints.add(EXTERNAL_EAST_USA_1);
    endpoints.add(EXTERNAL_ASIA_PACIFIC_SOU_1);
    endpoints.add(EXTERNAL_MIDDLE_EAST_1);
    endpoints.add(EXTERNAL_UK);
    return endpoints;
  }

  public static List<Endpoint> getClassicNetworkEndpoints() {
    List<Endpoint> endpoints = new ArrayList<>();
    endpoints.add(CLASSIC_CHINA_EAST_1);
    endpoints.add(CLASSIC_CHINA_EAST_2);
    endpoints.add(CLASSIC_CHINA_NORTH_2);
    endpoints.add(CLASSIC_CHINA_SOUTH_1);
    endpoints.add(CLASSIC_HONG_KONG);
    endpoints.add(CLASSIC_ASIA_PACIFIC_SE_1);
    endpoints.add(CLASSIC_ASIA_PACIFIC_SE_2);
    endpoints.add(CLASSIC_ASIA_PACIFIC_SE_3);
    endpoints.add(CLASSIC_ASIA_PACIFIC_SE_5);
    endpoints.add(CLASSIC_ASIA_PACIFIC_NE_1);
    endpoints.add(CLASSIC_CENTRAL_EUROPE_1);
    endpoints.add(CLASSIC_WEST_USA_1);
    endpoints.add(CLASSIC_EAST_USA_1);
    endpoints.add(CLASSIC_ASIA_PACIFIC_SOU_1);
    endpoints.add(CLASSIC_MIDDLE_EAST_1);
    endpoints.add(CLASSIC_UK);
    return endpoints;
  }

  public static List<Endpoint> getVPCEndpoints() {
    List<Endpoint> endpoints = new ArrayList<>();
    endpoints.add(VPC_CHINA_EAST_1);
    endpoints.add(VPC_CHINA_EAST_2);
    endpoints.add(VPC_CHINA_NORTH_2);
    endpoints.add(VPC_CHINA_SOUTH_1);
    endpoints.add(VPC_HONG_KONG);
    endpoints.add(VPC_ASIA_PACIFIC_SE_1);
    endpoints.add(VPC_ASIA_PACIFIC_SE_2);
    endpoints.add(VPC_ASIA_PACIFIC_SE_3);
    endpoints.add(VPC_ASIA_PACIFIC_SE_5);
    endpoints.add(VPC_ASIA_PACIFIC_NE_1);
    endpoints.add(VPC_CENTRAL_EUROPE_1);
    endpoints.add(VPC_WEST_USA_1);
    endpoints.add(VPC_EAST_USA_1);
    endpoints.add(VPC_ASIA_PACIFIC_SOU_1);
    endpoints.add(VPC_MIDDLE_EAST_1);
    endpoints.add(VPC_UK);
    return endpoints;
  }

}
