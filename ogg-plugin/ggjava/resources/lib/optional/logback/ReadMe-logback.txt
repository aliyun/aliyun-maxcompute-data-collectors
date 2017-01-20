To select logback as slf4j's log implementation,

(1) Copy the logback jars into "ggjava/resources/lib/optional/logback".
     * logback-core.jar
     * logback-classic.jar
    If logback is the implementation, they are added to the classpath.

(2) Select logback as the implementation, by either:
     * set the property (in the property file) gg.log=logback,
       and a default logback configuration is used.
     * OR, specify a logback configuration file as a JVM option
       jvm.bootoptions= ...-Dlogback.configurationFile=dirprm/my-logback.xml ...

