# DataHub Sink Plugin for Apache Flume

The DataHub sink streams events data into a DataHub topic. It has the following features:

- Map structured data to corresponding columns in the DataHub topic.
- Currently only support delimited/json/regex text data.
- Highly customizable.
- Compatible with all the features of Flume, such as fan-in and fan-out flows, contextual routing and backup routes (fail-over) for failed hops.

**NOTE: For non-developer users, please goto [(click here)](https://yq.aliyun.com/articles/66112).**

## Getting Started
---

### Requirements

To get started using this plugin, you will need three things:

1. JDK 1.8 or later (JDK 1.8 recommended)
2. Apache Maven 3.x  
3. Flume-NG 1.x (1.9 recommended)  (*[Home Page](https://flume.apache.org/index.html)*)

### Build the Package

Use maven to build the package:

```
$ cd flume-plugin/
$ mvn clean package -DskipTests
```

Wait until building success, the plugin will be **target/aliyun-flume-datahub-sink-x.x.x.tar.gz**.

### Use the Sink in Flume

```
$ tar zxvf aliyun-flume-datahub-sink-x.x.x.tar.gz
$ ls aliyun-flume-datahub-sink
lib    libext
```

Move the plugin flume-datahub-sink into the plugin directory of Flume (i.e., the folder **plugins.d/** under the Flume installation directory). If the plugin directory does not exist, create it at first:

```
$ mkdir {YOUR_FLUME_DIRECTORY}/plugins.d
$ mv aliyun-flume-datahub-sink {YOUR_FLUME_DIRECTORY}/plugins.d/
```

Optionally, you can check if the plugin is already in the directory:

```
$ ls {YOUR_FLUME_DIRECTORY}/plugins.d
aliyun-flume-datahub-sink
```

The DataHub sink should be available for Flume now. You can use this sink by set the type of the Flume sink to **com.aliyun.datahub.flume.sink.DatahubSink**. Details about the configure paramters of the Datahub sink are listed in [Sink Paramters](http://github.com/aliyun/aliyun-odps-flume-plugin/wiki/sink-parameter).

## Useful Links
---

- [Flume User Guide](https://flume.apache.org/FlumeUserGuide.html)

## Authors && Contributors
---
- [Ouyang Zhe](https://github.com/oyz)
- [Yang Hongbo](https://github.com/hongbosoftware)
- [Tian Li](https://github.com/tianliplus)
- [Li Ruibo](https://github.com/lyman)

## License
---

licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
