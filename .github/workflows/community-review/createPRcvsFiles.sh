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
# Take the prArray.json file and create a file $pr.csv that contains the login, state and when it was created.
#
# input:
#    prArray.json
#    $1 GIT_TOKEN
# output:
#    one csv file for each PR

# get pr numbers list
jq -jr '.[] |  .node.number, "-",  .node.timelineItems.pageInfo.hasNextPage,"\n"  '  prArray.json >prNumbersAndPaging.txt
# Process each pr separately in a loop

while read -r line;
do

  pr=${line%-*}
  hasNextPage=${line#*-}
  # find the node for our pr

  output=$pr.csv
  comma_join_expression="\",\""
  if [ $hasNextPage = "false" ]; then
      jq -r ".[] | select(.node.number==$pr) | .node.timelineItems.nodes | sort_by([.author.login, .createdAt]) | reverse | unique_by(.author.login) | .[] | [.author.login, .state, .createdAt] | join($comma_join_expression)" prArray.json >$output
  else
      ./getAllReviewsForPR.sh $1 $pr
      # we now have a file  allreviewsFor$pr.json
      jq -r ". | sort_by([.author.login, .createdAt]) | reverse | unique_by(.author.login) | .[] | [.author.login, .state, .createdAt] | join($comma_join_expression)" allreviewsFor$pr.json
  fi

done < prNumbersAndPaging.txt
