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
# community review gitaction - set the community review labels on PRs to show case community review activity

./getAllPRsFromGit.sh $1

# we should have a file called prArrays.json that is an array of node objects, one per PR
# each node contains the first page of a list of PRs.
./createPRcvsFiles.sh $1

# we now should have a csv files names <pr_number>.sh with rows of review information
# call this script to set community review labels appropriately for each PR
./processPRcsvFiles.sh $1
