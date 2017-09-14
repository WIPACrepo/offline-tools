
for var in "$@"
do
    python SubmitGCDJobs.py -s "$var" -e "$var"
done

