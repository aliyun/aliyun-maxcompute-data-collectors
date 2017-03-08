# Aliyun MaxCompute Data Collectors

[![Build Status](https://travis-ci.org/aliyun/aliyun-maxcompute-data-collectors.svg?branch=master)](https://travis-ci.org/aliyun/aliyun-maxcompute-data-collectors)

---

This project is a group of bigdata plugins for exchanging data with aliyun maxcompute.
The plugins contain flume-plugin, kettle-plugin, ogg-plugin and odps-sqoop.

## Requirements

- JDK 1.6 or later 
- Apache Maven 3.x

## Building the Sources

Clone the project from github:

``` 
$ git clone https://github.com/aliyun/aliyun-maxcompute-data-collectors.git
```

Build the sources using maven:

```
$ cd aliyun-maxcompute-data-collectors
$ mvn clean package -DskipTests=true  -Dmaven.javadoc.skip=true
```

Plugin packages are under each plugin subproject's `target` directroy.

## Usages

Please refer to **[Wiki](https://github.com/aliyun/aliyun-maxcompute-data-collectors/wiki)** of basic usages.

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
