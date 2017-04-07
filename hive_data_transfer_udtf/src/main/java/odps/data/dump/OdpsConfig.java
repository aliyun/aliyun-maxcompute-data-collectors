package odps.data.dump;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Random;

public class OdpsConfig {
    private String accessId;
    private String accessKey;
    private String odpsEndPoint;
    private String tunnelEndPoint;
    private String projectName;

    public static final String SEP = "#";
    public static final String ACCESS_ID = "access_id";
    public static final String ACCESS_KEY = "access_key";
    public static final String ODPS_ENDPOINT = "odps_endpoint";
    public static final String TUNNEL_ENDPOINT = "tunnel_endpoint";
    public static final String PROJECT_NAME = "project_name";

    private String getTunnelEndpoint(HashMap<String,String> configMap) {
        String tunnelEndPointList = configMap.get(TUNNEL_ENDPOINT);
        String[] endpoints = tunnelEndPointList.split(",");
        Random random = new Random();
        int index = random.nextInt(endpoints.length);
        System.out.println("tunnel index is : " + index);
        return endpoints[index];
    }

    public void init(String configFileName) throws IOException {
        HashMap<String, String> configMap = new HashMap<String, String>();
        readFile(configMap, configFileName);

        accessId = configMap.get(ACCESS_ID);
        accessKey = configMap.get(ACCESS_KEY);
        odpsEndPoint = configMap.get(ODPS_ENDPOINT);
        tunnelEndPoint = getTunnelEndpoint(configMap);
        projectName = configMap.get(PROJECT_NAME);
    }

    /**
     * config file format
     *
     * access_id#xxxxxxx
     * access_key#xxxxxx
     * odps_endpoint#xxxxxx
     * project_name#xxxxxxx
     * tunnel_endpoint#xxxxxx
     *
     * @param tbl
     * @param fname
     * @throws IOException
     */
    private void readFile(HashMap<String, String> tbl, String fname) throws IOException {
        FileInputStream fis = new FileInputStream(fname);
        InputStreamReader isr = new InputStreamReader(fis, "utf-8");
        BufferedReader br = new BufferedReader(isr);
        String tp = null;
        String[] tmp = null;
        while((tp = br.readLine()) != null) {
            tp = tp.trim();
            tmp = tp.split(SEP);
            tbl.put(tmp[0].toLowerCase(), tmp[1]);
        }
    }

    public String getAccessId() {
        return accessId;
    }

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getOdpsEndPoint() {
        return odpsEndPoint;
    }

    public void setOdpsEndPoint(String odpsEndPoint) {
        this.odpsEndPoint = odpsEndPoint;
    }

    public String getTunnelEndPoint() {
        return tunnelEndPoint;
    }

    public void setTunnelEndPoint(String tunnelEndPoint) {
        this.tunnelEndPoint = tunnelEndPoint;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }
}
