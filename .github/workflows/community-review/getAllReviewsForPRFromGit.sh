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
# This script issues paginated graphql calls using the github grapqhQL API to get information about
# all the reviews for a PRs
# input:
#     $1 git token to use
#     $2 pr number to get the reviews from
# output: a json file called $2-Reviews.json that contains all the timelineItem nodes.
REPO=apache
PROJECT=flink

cursor=""
hasNextPage=true

# TODO use order_by to order the results to the last hour in the graphQL?
GETREVIEWS_TEMPLATE="query {
 repository(owner: \\\"$REPO\\\" name: \\\"$PROJECT\\\") {
    pullRequest(number: $2) {
      id
      number
      timelineItems(first: 100 AFTER_CURSOR itemTypes: [PULL_REQUEST_REVIEW] ) {
                            nodes {
                              ... on PullRequestReview {
                                author {
                                    login
                                }
                                state
                                createdAt
                              }
                            }
                            pageInfo {
                               endCursor
                               hasNextPage
                            }
                          }
    }
  }
}"
count=-1
while [ "$hasNextPage" = "true" ]
do
    ((++count))
    if [[ -n $cursor ]]; then
      # remove exiting quotes in the string
      cursor=$(echo $cursor | sed 's/"//g')
      insertCursor=after:\\\\\"$cursor\\\\\",
    else
      insertCursor=""
    fi
    echo insertCursor=$insertCursor...
    script=$(echo "$GETREVIEWS_TEMPLATE" | sed -r "s/AFTER_CURSOR/$insertCursor/")
    script="$(echo $script)"   # the query should be a one-liner, without newlines
    curl -s  'Content-Type: application/json' \
      -H "Authorization: bearer $1" \
     -X POST -d "{ \"query\": \"$script\"}" https://api.github.com/graphql > restResponse.json

    jq  '.data.repository.pullRequest.timelineItems.nodes' restResponse.json  > cutdownrestResponse.json
    hasNextPage=$(cat restResponse.json | jq  '.data.repository.pullRequest.timelineItems.pageInfo.hasNextPage')
    cursor=$(cat restResponse.json | jq  '.data.repository.pullRequest.timelineItems.pageInfo.endCursor')

    echo issued review curl got hasNextPage: $hasNextPage and cursor: $cursor
    mv cutdownrestResponse.json $2review$count.json

done
echo count $count
# we need an expression like this ".[0] + .[1] ...."
for i in $(seq 0 $count);
do
    jqExpression+=" .[$i] +"
done

jqExpression="${jqExpression%?}"
echo jqExpression $jqExpression
jq -s "$jqExpression" $2review*.json > allreviewsFor$2.json
