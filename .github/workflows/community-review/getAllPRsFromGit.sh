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
# This script issues paginated graphql calls using the github grapqhQL API to get information about all the PRs
# input: a parameter that is the git token to use
# output: a json file called prArray.json that contains an array of node objects representing PRs including the firwt page of their reviews

REPO=apache
PROJECT=flink

cursor=""
hasNextPage=true
GETPRS_TEMPLATE="query {
 repository(owner: \\\"$REPO\\\" name: \\\"$PROJECT\\\") {
        pullRequests( first:100, AFTER_CURSOR states: [OPEN] ) {
            edges {
                node {
                    number
                        isDraft
                    timelineItems(first: 100, itemTypes: [PULL_REQUEST_REVIEW]) {
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
               cursor
            }
            pageInfo {
              endCursor
              hasNextPage
            }
          }
    }
}"
count=0
while [ "$hasNextPage" = "true" ]
do

    if [[ -n $cursor ]];
    then
      # remove exiting quotes in the string
      cursor=$(echo $cursor | sed 's/"//g')
      insertCursor=after:\\\\\"$cursor\\\\\",
    else
      insertCursor=""
    fi
    script=$(echo "$GETPRS_TEMPLATE" | sed -r "s/AFTER_CURSOR/$insertCursor/")
    script="$(echo $script)"   # the query should be a one-liner, without newlines
    curl -s  'Content-Type: application/json' \
      -H "Authorization: bearer $1" \
     -X POST -d "{ \"query\": \"$script\"}" https://api.github.com/graphql > restResponse.json
    jq  '.data.repository.pullRequests.edges ' restResponse.json >cutdownrestResponse.json
    hasNextPage=$(cat restResponse.json | jq  '.data.repository.pullRequests.pageInfo.hasNextPage')
    cursor=$(cat restResponse.json | jq  '.data.repository.pullRequests.pageInfo.endCursor')

    echo issued curl got hasNextPage: $hasNextPage and cursor: $cursor
    mv cutdownrestResponse.json response$count.json
   ((++count))
done
# merge the arrays

# we need an expression like this ".[0] + .[1] ...."
for i in $(seq 0 $count);
do
    jqExpression+=" .[$i] +"
done

jqExpression="${jqExpression%?}"

jq -s "$jqExpression" response*.json > prArray.json
