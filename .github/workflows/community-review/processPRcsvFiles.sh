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

# Process csv file in the current folder. They will have filenames of the pr number
# These csv files contain a row for each review, with the user, creation time and review state
# This script processes the file for each pr to obtain
#  community Approves
#  requestForChanges
#  committerApproves
#  communityReviews
#
# If there is 2 or more community approves, not request for changes and no committer approves we set
# label 'community-reviewed-LGTM' on the PR. If not and there has been a community review, label
# 'community-reviewed' is set.
# If one of the labels is set the other is unset.

# Inputs:
#    $1  GITHUB_TOKEN


for csv_file in *.csv;
do
  communityApproves=0
  requestForChanges=0
  committerApproves=0
  communityReviews=0
  while IFS=, read -r user state time
  do
      echo "Got:$user | $state | $time"
      hasReadRoleReview=false
      ./isUserCommitter.sh $1 $user
      #see if the user has read role
      if [ $? == 1 ]; then
          if [[ $state = "APPROVED" ]]; then
                      ((++committerApproves))
          fi
     else
          ((++communityReviews))
          if [[ $state = "APPROVED" ]]; then
             ((++communityApproves))
          fi
     fi
     if [[ $state = "CHANGES_REQUESTED" ]]; then
        ((++requestForChanges))
     fi
  done < $csv_file
  echo $pr communityApproves $communityApproves requestForChanges $requestForChanges committerApproves $committerApproves communityReviews $communityReviews

  # get rid of suffix starting with . i.e. remove the file type leaving the filename
  pr=${csv_file%.*}

  LGTM_LABEL="community-reviewed-LGTM"
  COMMUNITY_REVIEW_LABEL="community-reviewed"

  if [[ $communityApproves -ge  "2" && $requestForChanges = 0 && $committerApproves = 0 ]]; then
     ./mutateLabel.sh $1 $LGTM_LABEL "POST" $pr
     ./mutateLabel.sh $1 $COMMUNITY_REVIEW_LABEL "DELETE" $pr
     echo Set Label $LGTM_LABEL for PR $pr
  elif [[ communityReviews -ge "0" ]]; then
    ./mutateLabel.sh $1 $COMMUNITY_REVIEW_LABEL "POST" $pr
    ./mutateLabel.sh $1 $LGTM_LABEL "DELETE" $pr
    echo Set Label $COMMUNITY_REVIEW_LABEL for PR $pr
  fi
done
