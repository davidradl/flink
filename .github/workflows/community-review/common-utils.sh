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

# ======================================
# community-review-utils.sh
# Contains utility functions
# ======================================

# =============================================================================
# Send a POST request to the GitHub GraphQL API and output the response.
# Use in combination with the common_check_github_graphql_response function.
# Arguments:
#   $1 - JSON payload to be sent with the request
#   $2 - GitHub API token for authentication
# Usage:
#    restResponse="$(common_call_github_graphql_api "$payload" "$token")"
#    common_check_github_graphql_response "$restResponse"
# =============================================================================
common_call_github_graphql_api() {
  local payload="${1?missing payload}"
  local token="${2?missing token}"

  curl --fail --no-progress-meter \
    -X POST \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer ${token}" \
    -d "${payload}" \
    "https://api.github.com/graphql"
}

# =============================================================================
# Check the response of the cURL request. Exit in case of error.
# Arguments:
#   $1 - response from callAPI 
# =============================================================================
common_check_github_graphql_response() {
  local response="${1?missing response}"
  
  # check if response contains a valid JSON
  if jq -e . >/dev/null 2>&1 <<<"$response"; then
    # The cURL request can be successful, but still return an error if the data it receives is
    # incorrect, such as a malformed payload.
    if [ "$(jq 'has("errors") and .errors != null' <<< "$response")" = "true" ]; then
      # display the error and terminate
      echo "ERROR received: $response"; exit 1;
    fi
  else
    # The cURL request failed and received no response. cURL already displayed the error.
    exit 1
  fi
}

# =============================================================================
# Replaces a template string {{TEMPLATE_NAME}} with a specified value in a given text.
# Expected arguments 
#   $1 - The original text to search and substitute the template string.
#   $2 - The name of the template to replace.
#   $3 - The value to substitute in place of the template string.
# =============================================================================
common_replace_template_value() {
  local text=${1?missing text to update}
  local templateName=${2?missing template nam}
  local value=${3?missing value}

  echo "$text" | sed -r "s/{{${templateName}}}/${value}/"
}

# =============================================================================
# Count number of item of a JON array
# Expected arguments 
# =============================================================================
common_JSONArrayLength() {
  local jsonArray=${1?missing array}
  echo "${jsonArray}" | jq 'length'
}
