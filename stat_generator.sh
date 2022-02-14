#!/bin/bash

# dev database
DATABASE="bolt://intact-neo4j-001.ebi.ac.uk:7687"
USER="neo4j"
PW="neo4j123"
GIT_REP="statistics_dev"

# prod database
#database="bolt://intact-neo4j-003.ebi.ac.uk:7687"
#git_rep="statistics_prod"

git checkout ${GIT_REP}
pip3 install -r requirements.txt
if [ $? -ne 0 ]; then
  echo "Requirements installation failed, please take a look at the requirements."
fi

python3 statistics_generator.py --database ${DATABASE} --user ${USER} --pw ${PW}
if [ $? -eq 0 ]; then
  echo "Script executed successfully"
  git add .
  git commit -a -m "New statistics files added on $(date)"
  git push
else
  echo "Script exited with an error." >&2
fi

git checkout main
