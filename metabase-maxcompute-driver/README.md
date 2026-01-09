# Metabase MaxCompute Driver

> **Note**: This driver is in its early stages and has not undergone extensive testing. Features
> have been validated through self-testing, but production environment testing has not been
> completed.
> The developer is not an advanced Metabase user, so full functionality cannot be guaranteed. If you
> encounter any issues, please open an issue, and we will address it as soon as possible.

## Installation

Beginning with Metabase 0.32, drivers must be stored in a `plugins` directory in the same directory
where `metabase.jar` is, or you can specify the directory by setting the environment
variable `MB_PLUGINS_DIR`.

### Download Metabase Jar and Run

1. Download a fairly recent Metabase binary release (jar file) from
   the [Metabase distribution page](https://metabase.com/start/jar.html).
2. Build the MaxCompute driver jar or download the pre-build Jar.
3. Create a directory and copy the `metabase.jar` to it.
4. In that directory create a sub-directory called `plugins`.
5. Copy the MaxCompute driver jar to the `plugins` directory.
6. Make sure you are in the directory where your `metabase.jar` lives.
7. Run `java -jar metabase.jar`.

In either case, you should see a message on startup similar to:

```
04-15 06:14:08 DEBUG plugins.lazy-loaded-driver :: Registering lazy loading driver :maxcompute...
04-15 06:14:08 INFO driver.impl :: Registered driver :maxcompute (parents: [:sql-jdbc]) 🚚
```

## Configuring

Once you've started up Metabase, go to add a database and select "MaxCompute".
You'll need to provide the [MaxCompute endpoint](https://help.aliyun.com/zh/maxcompute/user-guide/endpoints), access key, secret key, and project information.

Please note:

- The provided project must be in the same region you specify.
- The initial sync can take some time depending on how many databases and tables you have.

If you need an example policy for providing read-only access to your customer-base, make sure to
consult the Alibaba Cloud documentation.

## Contributing

### Prerequisites

- [Install Metabase core](https://github.com/metabase/metabase/wiki/Writing-a-Driver:-Packaging-a-Driver-&-Metabase-Plugin-Basics#installing-metabase-core-locally)

### Build from Source

1. Clone the repository:

```shell
git clone git@github.com:aliyun/aliyun-maxcompute-data-collectors.git
cd aliyun-maxcompute-data-collectors/metabase-maxcompute-driver
```
2. replace the metabase source code directory in `deps.edn`
3. Build the project

```shell
clojure -X:build
```

You should now have a `maxcompute.metabase-driver.jar` file in the `target` directory.

4. Download a fairly recent Metabase binary release (jar file) from
   the [Metabase distribution page](https://metabase.com/start/jar.html).

5. Let's assume we download `metabase.jar` to `~/metabase/` and we built the project above. Copy the
   built jar to the Metabase plugins directly and run Metabase from there!

```shell
TARGET_DIR=~/metabase
mkdir ${TARGET_DIR}/plugins/
cp target/maxcompute.metabase-driver.jar ${TARGET_DIR}/plugins/
cd ${TARGET_DIR}/
java -jar metabase.jar
```

You should see a message on startup similar to:

```
2019-05-07 23:27:32 INFO plugins.lazy-loaded-driver :: Registering lazy loading driver :maxcompute...
2019-05-07 23:27:32 INFO metabase.driver :: Registered driver :maxcompute (parents: #{:sql-jdbc}) 🚚
```

### Testing

Testing is not yet implemented for this driver. However, we encourage users to report any issues
encountered during usage. Testing will be added in future updates.

### Contributing
This driver is developed based on Metabase version 0.50. If you find it incompatible with a specific version, please don't hesitate to contact us.
Any form of contribution is also welcome.

## Issues

If you encounter any issues or have questions, please open
an [issue](https://github.com/aliyun/aliyun-maxcompute-data-collectors/issues) on our GitHub page. We will address it as soon
as possible.

Thank you for using the Metabase MaxCompute Driver!
