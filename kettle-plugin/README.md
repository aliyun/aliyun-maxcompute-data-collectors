
# MaxCompute Input/Output Plugin for Kettle

The maxcompute input/output plugin for kettle is implemented through tunnel. It has the following features:

- Map structured data to corresponding columns in the maxcompute table.
- Batch uploads and downloads.
- Compatible with all the features of Kettle, such as mysql, oracle input and output streams.

## Getting Started
### Requirements

To get started using this plugin, you will need three things:
1. JDK 1.6 or later (JDK 1.7 recommended)
2. Apache Maven 3.x
3. Kettle (DATA INTEGRATION) 7.0 (*[Home Page](http://community.pentaho.com/projects/data-integration/)*)

---

### Build the Package

Use maven to build the package:

```
$ cd aliyun-odps-kettle-plugin/
$ mvn clean package -DskipTests
```

----

## Useful Links

+ [Kettle Download](http://community.pentaho.com/projects/data-integration/)
+ [Hello World Example](http://wiki.pentaho.com/display/EAI/03.+Hello+World+Example)

+ [A Kettle Related Blog](http://type-exit.org/adventures-with-open-source-bi/tag/kettle/page/4/)
+ [Developing a Custom Kettle Plugin](http://type-exit.org/adventures-with-open-source-bi/2010/06/developing-a-custom-kettle-plugin-looking-up-values-in-voldemort/)
