#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
set -e

# override env to use Java 17 to for build instead default Java 8
# path to JDK is taken from https://github.com/apache/flink-connector-shared-utils/blob/ci_utils/docker/base/Dockerfile#L37-L40
export JAVA_HOME=$JAVA_HOME_17_X64
export PATH=$JAVA_HOME_17_X64/bin:$PATH

echo Script run!
