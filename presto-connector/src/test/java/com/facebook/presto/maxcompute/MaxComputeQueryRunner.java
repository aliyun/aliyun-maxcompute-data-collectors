/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.maxcompute;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class MaxComputeQueryRunner
{
    private MaxComputeQueryRunner() {}

    private static final DockerImageName MAXCOMPUTE_IMAGE =
            DockerImageName.parse("maxcompute/maxcompute-emulator:v0.0.6");

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        Logger log = Logger.get(MaxComputeQueryRunner.class);

        DistributedQueryRunner queryRunner = null;
        try {
            //noinspection resource
            queryRunner =
                    createMaxComputeEmulatorQueryRunner(Optional.of("8080"));
        }
        catch (Throwable t) {
            log.error(t);
            System.exit(1);
        }
        Thread.sleep(10);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    public static String MAXCOMPUTE_EMULATOR_ENDPOINT;

    static DistributedQueryRunner createMaxComputeQueryRunner(Optional<String> serverPort, String endpoint, String accessId, String accessKey, String project)
            throws Exception
    {
        Map<String, String> serverConfig = serverPort
                .map(s -> ImmutableMap.of("http-server.http.port", s))
                .orElse(ImmutableMap.of());

        Map<String, String> catalogConfig = new HashMap<>();
        catalogConfig.put("odps.access.id", accessId);
        catalogConfig.put("odps.access.key", accessKey);
        catalogConfig.put("odps.project.name", project);
        catalogConfig.put("odps.end.point", endpoint);

        Session session = testSessionBuilder().setCatalog("maxcompute").setSchema("default").build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).setExtraProperties(serverConfig).build();

        queryRunner.installPlugin(new MaxComputePlugin());
        queryRunner.createCatalog("maxcompute", "maxcompute", catalogConfig);

        return queryRunner;
    }

    static DistributedQueryRunner createMaxComputeEmulatorQueryRunner(Optional<String> serverPort)
            throws Exception
    {
        GenericContainer<?> maxcompute =
                new GenericContainer<>(MAXCOMPUTE_IMAGE)
                        .withExposedPorts(8080)
                        .waitingFor(
                                Wait.forLogMessage(".*Started MaxcomputeEmulatorApplication.*\\n", 1))
                        .withLogConsumer(frame -> System.out.print(frame.getUtf8String()));
        maxcompute.start();
        // wait one seconds for maxcompute to start completely
        Thread.sleep(1000);

        String endpoint = getEndpoint(maxcompute);
        MAXCOMPUTE_EMULATOR_ENDPOINT = endpoint;
        sendPOST(endpoint + "/init", endpoint);

        return createMaxComputeQueryRunner(serverPort, endpoint, "mock", "mock", "emulator");
    }

    private static String getEndpoint(GenericContainer<?> maxcompute)
    {
        String ip;
        if (maxcompute.getHost().equals("localhost")) {
            try {
                ip = InetAddress.getLocalHost().getHostAddress();
            }
            catch (UnknownHostException e) {
                ip = "127.0.0.1";
            }
        }
        else {
            ip = maxcompute.getHost();
        }
        return "http://" + ip + ":" + maxcompute.getFirstMappedPort();
    }

    static void sendPOST(String postUrl, String postData)
            throws Exception
    {
        URL url = new URL(postUrl);

        HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
        httpURLConnection.setRequestMethod("POST");
        httpURLConnection.setDoOutput(true);
        httpURLConnection.setRequestProperty("Content-Type", "application/json");
        httpURLConnection.setRequestProperty("Content-Length", String.valueOf(postData.length()));

        try (OutputStream outputStream = httpURLConnection.getOutputStream()) {
            outputStream.write(postData.getBytes("UTF-8"));
            outputStream.flush();
        }
        int responseCode = httpURLConnection.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new RuntimeException("POST request failed with response code: " + responseCode);
        }
    }
}
