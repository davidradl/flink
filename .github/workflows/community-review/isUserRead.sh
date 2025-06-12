!/usr/bin/env bash
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

# check the push permission of the user - committers have push permission

# input:
#     $1 github token
#     $1 user name
# output:
#     return 1 if community member - i.e. no push permission
#     return 0 if committer


REPO=apache
PROJECT=flink

file_name="pushperm.txt"

if [ -e "$file_name" ]; then

  while IFS=, read -r user pushperm
  do
      echo "Got:$user | $pushperm"
      if [[ "$user" = $2 ]]; then
         PUSH_PERMISSION=$pushperm
         break
      fi

  done < $file_name
fi

if [ -z "$PUSH_PERMISSION" ]; then
   echo rest call
   PUSH_PERMISSION=$(curl -s -L \
     -H "Accept: application/vnd.github+json" \
     -H "Authorization: Bearer $1"\
     -H "X-GitHub-Api-Version: 2022-11-28" \
       https://api.github.com/repos/$REPO/$PROJECT/collaborators/$2/permission | jq -r '.user.permissions.push')
   # write line to file
   echo "Write to file $2,PUSH_PERMISSION"
   echo "$2,$PUSH_PERMISSION">>$file_name
fi


if [ "$PUSH_PERMISSION" == "false" ];
then
   exit 1
else
   exit 0
fi
