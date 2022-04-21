#!/usr/bin/env bash

if [ $# -eq 0 ]
  then
    echo "Version not specified"
    exit 1
fi

echo "Preparing tag $1"

git fetch --tags

# Uncomment to remove tag with the same name
# echo "Removing obsolete tag..."
# git tag -d $1
# git push origin :refs/tags/$1

git status

echo "Creating new tag..."
git tag -a $1 -m "Create new RC tag: $1"
git push origin $1

echo " "
echo "======================================================"
echo "RC tag should be created."
echo "Please check results at "
echo "https://gitbox.apache.org/repos/asf?p=ignite.git;a=tags"