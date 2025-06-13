# check the push permission of the user - committers have push permission

# input:
#     $1 github token
#     $2 user name
# output:
#     return 1 if committer
#     return 0 if not a committer


REPO=apache
PROJECT=flink

file_name="pushperm.txt"

if [ -e "$file_name" ]; then

  while IFS=, read -r user pushperm
  do
      if [[ "$user" = $2 ]]; then
         PUSH_PERMISSION=$pushperm
         break
      fi

  done < $file_name
fi

if [ -z "$PUSH_PERMISSION" ]; then
   PUSH_PERMISSION=$(curl -s -L \
     -H "Accept: application/vnd.github+json" \
     -H "Authorization: Bearer $1"\
     -H "X-GitHub-Api-Version: 2022-11-28" \
       https://api.github.com/repos/$REPO/$PROJECT/collaborators/$2/permission | jq -r '.user.permissions.push')
   # write line to file
   echo "Write to file $2,PUSH_PERMISSION"
   echo "$2,$PUSH_PERMISSION">>$file_name
fi


if [ "$PUSH_PERMISSION" == "true" ]; then
   exit 1
else
   exit 0
fi
