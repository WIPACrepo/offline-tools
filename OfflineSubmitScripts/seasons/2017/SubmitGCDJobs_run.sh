
for var in "$@"
do
    python SubmitGCDJobs.py -r -s "$var" -e "$var"
done

