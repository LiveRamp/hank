#!/bin/sh

git log | head -1 | awk '{print $2}' > build/git-commit.txt