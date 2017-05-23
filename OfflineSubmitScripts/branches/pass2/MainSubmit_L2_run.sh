
for var in "$@"
do
    python MainSubmit_L2.py --resubmission -s "$var"
done

