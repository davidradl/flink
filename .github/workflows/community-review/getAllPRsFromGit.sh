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
# This script issues paginated graphql calls using the github graphQL API to get information about all the PRs
# input: a parameter that is the git token to use for all curl calls to github
# output: a json file called prArray.json that contains an array of node objects representing PRs including the first page of their reviews
set -e

# Source the utility functions
source "$(dirname "$0")/common-utils.sh"

GETPRS_TEMPLATE='{
  "query":
    "query {
      repository(owner: \"{{REPO_OWNER}}\" name: \"{{REPO_NAME}}\") {
        pullRequests(first:100, {{AFTER_CURSOR}} states: [OPEN]) {
          edges {
            node {
              number
              isDraft
              timelineItems(first: 5, itemTypes: [PULL_REQUEST_REVIEW]) {
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
}'

# =============================================================================
# Arguments:
#   $1 - GitHub API token for authentication, with permission to list pull requests.
#   $2 - (optional) output file. Default: prArray.json
#   $3 - (optional) owner of the repository. Default: apache
#   $4 - (optional) name of the repository. Default: flink
# =============================================================================
main() {
    local token="${1?missing token}"
    local outputFile="${2:-prArray.json}"
    local repositoryOwner="${3:-apache}"
    local repositoryName="${4:-flink}"
    
    echo "- repositoryOwner: ${repositoryOwner}"
    echo "- repositoryName: ${repositoryName}"
    
    # prepare payload template
    local payloadTemplate
    payloadTemplate=$(echo "$GETPRS_TEMPLATE" | tr -d '\n')  # the query should be a one-liner, without newlines
    payloadTemplate="$(common_replace_template_value "$payloadTemplate" "REPO_OWNER" "${repositoryOwner}")"
    payloadTemplate="$(common_replace_template_value "$payloadTemplate" "REPO_NAME" "${repositoryName}")"
    
    local pullRequests="[]"
    local hasNextPage=true
    local cursor=""
    local payload

    while [ "$hasNextPage" = "true" ]
    do  
      if [[ -n $cursor ]]; then
        payload="$(common_replace_template_value "$payloadTemplate" "AFTER_CURSOR" "after: \\\\\"${cursor}\\\\\",")"
      else
        payload="$(common_replace_template_value "$payloadTemplate" "AFTER_CURSOR" "")"
      fi
     
      echo "Requesting pull requests information..."
      restResponse="$(common_call_github_graphql_api "$payload" "$token")"
      common_check_github_graphql_response "$restResponse"
      receivedPullRequests="$(jq '.data.repository.pullRequests.edges' <<< "$restResponse")"    
      
      echo "- filtering $(common_JSONArrayLength "$receivedPullRequests") received pull requests..."  
      receivedPullRequests=$(jq '[.[] | select((.node.isDraft = false) and (.node.timelineItems.nodes | type != "array" or length > 0))]' <<< "$receivedPullRequests")
      echo "- after filtering: $(common_JSONArrayLength "$receivedPullRequests")"
      
      pullRequests=$(jq --argjson a1 "$pullRequests" --argjson a2 "$receivedPullRequests" '$a1 + $a2' <<< '{}')   
      
      hasNextPage=$(jq  '.data.repository.pullRequests.pageInfo.hasNextPage' <<< "$restResponse")
      cursor=$(jq -r '.data.repository.pullRequests.pageInfo.endCursor' <<< "$restResponse")
      echo "- hasNextPage: ${hasNextPage} / cursor: ${cursor}"
    done
       
    echo "saving $(common_JSONArrayLength "$pullRequests") pull requests to ${outputFile} ..."
    echo "${pullRequests}" | jq  > "${outputFile}"
    
    echo "done."
}

main "$@"
