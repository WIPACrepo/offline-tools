
for var in "$@"
do
    python MainSubmit_L2.py --resubmission --use-std-GCDs -s "$var"
done

