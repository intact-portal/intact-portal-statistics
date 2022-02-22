#!/bin/bash

# Defaults
USER='neo4j'

help() {
  # Display Help
  echo "Description of the required inputs of this script."
  echo
  echo "The following inputs are required -p and -r:"
  echo "r     The desired repository - 'dev' or 'prod'"
  echo "p     The Neo4J password connected to the repository"
  echo
}

while getopts p:r:h flag; do
  case "${flag}" in
  p) PW="${OPTARG}" ;;
  r) REPOSITORY="${OPTARG}" ;;
  h) help ;;
  \?)
    echo "Unknown option: -$OPTARG, add -h for help" >&2
    exit 1
    ;;
  :)
    echo "Missing option argument for -$OPTARG, add -h for help" >&2
    exit 1
    ;;
  *)
    echo "Unimplemented option: -$OPTARG, add -h for help" >&2
    exit 1
    ;;
  esac
done

# Control mandatory arguments
if [ ! "$PW" ] || [ ! "$REPOSITORY" ]; then
  echo "arguments -p and -r must be provided, add -h for help."
  echo >&2
  exit 1
fi

# Check what repository to use, dev or prod. And set the correct database and branch for it
if [[ "$REPOSITORY" == "dev" ]]; then
  DATABASE="bolt://intact-neo4j-001.ebi.ac.uk:7687"
  GIT_REP="statistics_dev"
elif [[ "$REPOSITORY" == "prod" ]]; then
  DATABASE="bolt://intact-neo4j-003.ebi.ac.uk:7687"
  GIT_REP="statistics_prod"
else
  echo "Incorrect argument for -r, add -h for help."
fi

# set brench and install requirements
git checkout ${GIT_REP}
pip3 install -r requirements.txt
if [ $? -ne 0 ]; then
  echo "Requirements installation failed, please take a look at the requirements."
  exit 1
fi

# Run python script
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
