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

package org.apache.spark.sql.odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.BearerTokenAccount;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.utils.StringUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import scala.UninitializedFieldError;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.aliyun.odps.table.utils.ConfigConstants.*;

public class OdpsClient {

    private static final Logger logger = Logger.getLogger(OdpsClient.class);

    private static OdpsClient odpsClient = null;

    private final Odps odps;
    private final ConcurrentHashMap<String, String> settings;
    private final Object odpsLock = new Object();
    private final CloseableHttpClient httpClient;
    private final String proxyUrl;
    private final EnvironmentSettings.ExecutionMode mode;

    private final static String ODPS_ACCESS_ID = "odps.access.id";
    private final static String ODPS_ACCESS_KEY = "odps.access.key";
    private final static String ODPS_ACCESS_SECURITY_TOKEN = "odps.access.security.token";

    private final static String ODPS_END_POINT = "odps.end.point";
    private final static String ODPS_RUNTIME_END_POINT = "odps.runtime.end.point";
    private final static String ODPS_TUNNEL_END_POINT = "odps.tunnel.end.point";
    private final static String ODPS_TUNNEL_QUOTA_NAME = "odps.tunnel.quota.name";
    private final static String ODPS_TUNNEL_TAGS = "odps.tunnel.tags";

    private final static String ODPS_PROJECT_NAME = "odps.project.name";
    private final static String ODPS_TABLE_EXECUTION_MODE = "odps.table.execution.mode";

    private final static String META_LOOKUP_NAME = "META_LOOKUP_NAME";
    private final static String REFRESH_INTERVAL_SECONDS = "odps.cupid.bearer.token.refresh.interval.seconds";
    private final static String ODPS_BEARER_TOKEN_ENABLE = "odps.cupid.bearer.token.enable";

    private final static String ODPS_TUNNEL_READ_TIMEOUT = "odps.tunnel.read.timeout.seconds";
    private final static String ODPS_TUNNEL_CONNECT_TIMEOUT = "odps.tunnel.connect.timeout.seconds";
    private final static String ODPS_TUNNEL_MAX_RETRIES = "odps.tunnel.max.retries";
    private final static String ODPS_TUNNEL_RETRY_WAIT_TIME = "odps.tunnel.retry.wait.seconds";

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final Map<String, String> options;
        private boolean loadDefaults;

        public Builder() {
            this.options = new HashMap<>();
            this.loadDefaults = false;
        }

        public Builder config(String key, String value) {
            this.options.put(key, value);
            return this;
        }

        public Builder config(Configuration configuration) {
            configuration.forEach(entry -> options.put(entry.getKey(), entry.getValue()));
            return this;
        }

        public Builder loadDefaultsConf(boolean loadDefaultsConf) {
            this.loadDefaults = loadDefaultsConf;
            return this;
        }

        public OdpsClient getOrCreate() {
            synchronized (OdpsClient.class) {
                if (odpsClient == null) {
                    try {
                        odpsClient = new OdpsClient(this);
                        odpsClient.startRefreshOdps();
                    } catch (Exception ex) {
                        throw new IllegalArgumentException(
                                "Get odps client failed: " + ex.getMessage());
                    }
                }
                // TODO: modify settings?
                return odpsClient;
            }
        }
    }

    public OdpsClient(Builder builder) {
        this.settings = new ConcurrentHashMap<>();
        builder.options.forEach(settings::put);
        loadDefaultSettings(builder.loadDefaults);
        httpClient = HttpClientBuilder.create().build();
        proxyUrl = initProxyUrl();
        odps = initOdps();
        mode = initTableExecutionMode();
    }

    public Odps odps() {
        synchronized (this.odpsLock) {
            return this.odps;
        }
    }

    public Optional<String> getSettings(String key) {
        return Optional.ofNullable(settings.get(key));
    }

    public static OdpsClient get() {
        synchronized (OdpsClient.class) {
            if (odpsClient == null) {
                throw new UninitializedFieldError("Get odps client failed");
            }
            return odpsClient;
        }
    }

    public EnvironmentSettings getEnvironmentSettings() {
        Odps odps = odps().clone();
        Credentials credentials = Credentials.newBuilder()
                .withAccount(odps.getAccount())
                .withAppAccount(odps.getAppAccount())
                .withAppStsAccount(odps.getAppStsAccount())
                .build();
        RestOptions restOptions = RestOptions.newBuilder()
                .withReadTimeout(Integer.parseInt(getSettings(ODPS_TUNNEL_READ_TIMEOUT).orElse("300")))
                .withConnectTimeout(Integer.parseInt(getSettings(ODPS_TUNNEL_CONNECT_TIMEOUT).orElse("20")))
                .withRetryTimes(Integer.parseInt(getSettings(ODPS_TUNNEL_MAX_RETRIES).orElse("5")))
                .withRetryWaitTimeInSeconds(Integer.parseInt(getSettings(ODPS_TUNNEL_RETRY_WAIT_TIME).orElse("5")))
                .build();
        EnvironmentSettings.Builder env = EnvironmentSettings.newBuilder()
                .withDefaultProject(odps.getDefaultProject())
                .withServiceEndpoint(odps.getEndpoint())
                .withCredentials(credentials)
                .withRestOptions(restOptions);
        if (mode.equals(EnvironmentSettings.ExecutionMode.LOCAL)) {
            env.inLocalMode();
        } else if (mode.equals(EnvironmentSettings.ExecutionMode.REMOTE)) {
            env.inRemoteMode();
        } else {
            env.inAutoMode();
        }
        getSettings(ODPS_TUNNEL_END_POINT).ifPresent(env::withTunnelEndpoint);
        getSettings(ODPS_TUNNEL_QUOTA_NAME).ifPresent(env::withQuotaName);
        getSettings(ODPS_TUNNEL_TAGS).ifPresent(input -> {
            if (!input.isEmpty()) {
                env.withTags(Arrays.asList(input.split(",")));
            }
        });
        return env.build();
    }

    private Odps initOdps() {
        Account account = getAccount();
        Odps odps = new Odps(account);
        String lookupName = System.getenv(META_LOOKUP_NAME);
        if (lookupName == null) {
            odps.setEndpoint(settings.get(ODPS_END_POINT));
        } else {
            odps.setEndpoint(settings.getOrDefault(ODPS_RUNTIME_END_POINT, settings.get(ODPS_END_POINT)));
        }
        odps.setDefaultProject(settings.get(ODPS_PROJECT_NAME));
        return odps;
    }

    private boolean isBearerTokenEnable() {
        return System.getenv(META_LOOKUP_NAME) != null
                && Boolean.parseBoolean(settings.getOrDefault(ODPS_BEARER_TOKEN_ENABLE, "true"));
    }

    private void startRefreshOdps() {
        if (isBearerTokenEnable()) {
            long interval = Long.parseLong(settings.getOrDefault(REFRESH_INTERVAL_SECONDS, "600"));
            Timer timer = new Timer("OdpsClientBearerTokenRefresher", true);
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    refreshOdps();
                }
            }, 0, interval * 1000);
        }
    }

    private void refreshOdps() {
        Account account = this.getAccount();
        synchronized (this.odpsLock) {
            this.odps.setAccount(account);
        }
    }

    private Account getAccount() {
        if (isBearerTokenEnable()) {
            try {
                String bearerToken = getBearerToken();
                if (bearerToken != null) {
                    return new BearerTokenAccount(bearerToken);
                }
            } catch (Throwable ex) {
                logger.error(
                        String.format(
                                "initialize BearerTokenAccount failed with %s, fallback to use AliyunAccount", ex.getMessage()
                        )
                );
            }
        }
        return getDefaultAccount();
    }

    private Account getDefaultAccount() {
        if (settings.containsKey(ODPS_ACCESS_SECURITY_TOKEN) &&
                StringUtils.isNotBlank(settings.get(ODPS_ACCESS_SECURITY_TOKEN))) {
            return new StsAccount(settings.get(ODPS_ACCESS_ID),
                    settings.get(ODPS_ACCESS_KEY),
                    settings.get(ODPS_ACCESS_SECURITY_TOKEN));
        } else {
            return new AliyunAccount(settings.get(ODPS_ACCESS_ID),
                    settings.get(ODPS_ACCESS_KEY));
        }
    }

    private void loadDefaultSettings(boolean loadDefaultsSettings) {
        if (!loadDefaultsSettings) {
            return;
        }
        URL url = null;
        String confPath = System.getenv("ODPS_CONF_FILE");
        if (confPath != null && !confPath.trim().isEmpty()) {
            File confFile = new File(confPath);
            if (confFile.exists()) {
                try {
                    url = confFile.toURI().toURL();
                } catch (MalformedURLException e) {
                    logger.debug("cannot find user defined odps config file: " + confPath);
                }
            }
        }
        if (url == null) {
            url = OdpsClient.class.getClassLoader().getResource("odps.conf");
        }

        Properties props = null;
        if (url != null) {
            props = new Properties();
            try {
                InputStreamReader reader = new InputStreamReader(url.openStream(), StandardCharsets.UTF_8);
                props.load(reader);
            } catch (IOException ex) {
                props = null;
            }
        }
        if (props == null) {
            logger.debug("load odps default configs failed.");
            return;
        }

        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("odps.")) {
                String value = props.getProperty(key).trim();
                if (!value.isEmpty()) {
                    settings.put(key, value);
                }
            }
        }
    }

    private String initProxyUrl() {
        String mode = System.getenv(MAX_STORAGE_MODE);
        if (StringUtils.isNullOrEmpty(mode)) {
            return null;
        }

        String prefix = System.getenv(MAX_STORAGE_DATA_PROXY_PREFIX);
        String port = System.getenv(MAX_STORAGE_DATA_PROXY_PORT);

        Preconditions.checkString(port, "MAX_STORAGE_DATA_PROXY_PORT");

        String localHostPort;
        if (StringUtils.isNullOrEmpty(prefix)) {
            localHostPort = "http://127.0.0.1:" + port + "/v1";
        } else {
            localHostPort = "http://127.0.0.1:" + port + "/" + prefix + "/v1";
        }
        return localHostPort;
    }

    private EnvironmentSettings.ExecutionMode initTableExecutionMode() {
        Optional<String> executionMode = getSettings(ODPS_TABLE_EXECUTION_MODE);
        if (executionMode.isPresent()) {
            String mode = executionMode.get();
            if (mode.equals("local")) {
                return EnvironmentSettings.ExecutionMode.LOCAL;
            } else if (mode.equals("remote")) {
                return EnvironmentSettings.ExecutionMode.REMOTE;
            }
        }
        return EnvironmentSettings.ExecutionMode.AUTOMATIC;
    }

    private String getBearerToken() throws Exception {
        String bearerToken;
        try {
            bearerToken = postRpcCall("get_bearer_token", "".getBytes("UTF-8"));
        } catch (Exception ex) {
            logger.warn("getBearerToken failed, fallback to use BEARER_TOKEN_INITIAL_VALUE", ex);
            if (System.getenv("BEARER_TOKEN_INITIAL_VALUE") != null) {
                bearerToken = System.getenv("BEARER_TOKEN_INITIAL_VALUE");
            } else {
                throw ex;
            }
        }
        return bearerToken;
    }

    private String postRpcCall(String method, byte[] parameter) {
        if (StringUtils.isNullOrEmpty(proxyUrl)) {
            return null;
        }
        HttpPost httpPost = new HttpPost(proxyUrl + "/rpc/" + method);
        CloseableHttpResponse response = null;
        try {
            ByteArrayEntity entity = new ByteArrayEntity(Base64.encodeBase64(parameter));
            httpPost.setEntity(entity);
            response = httpClient.execute(httpPost);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                HttpEntity responseEntity = response.getEntity();
                return EntityUtils.toString(responseEntity);
            } else {
                logger.error("RestClient callMethod fail, error code:" + response.getStatusLine().getStatusCode());
                return null;
            }
        } catch (Exception e) {
            logger.error("RestClient callMethod fail", e);
        }
        return null;
    }
}
