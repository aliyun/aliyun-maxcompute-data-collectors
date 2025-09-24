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

package org.apache.spark.sql.execution.datasources.v2.odps

import java.io.IOException
import java.util.Properties
import scala.util.Using

object ConfigLoader {

  private lazy val props: Properties = loadConfig()

  def loadConfig(): Properties = {
    val props = new Properties()
    Using(getClass.getClassLoader.getResourceAsStream("spark_test.conf")) { is =>
      if (is != null) {
        props.load(is)
      } else {
        throw new IOException("Cannot find spark_test conf")
      }
    }

    props
  }

  def getProject(): String = {
    props.getProperty("odps.project.name")
  }

  def getEndpoint(): String = {
    props.getProperty("odps.end.point")
  }

  def getAccessId(): String = {
    props.getProperty("odps.access.id")
  }

  def getAccessKey(): String = {
    props.getProperty("odps.access.key")
  }

  def getProps: Properties = props
}
