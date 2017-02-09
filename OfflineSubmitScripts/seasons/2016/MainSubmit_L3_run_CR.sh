
for var in "$@"
do
     python MainSubmit_L3.py --sourcedatasetid 1871 --destinationdatasetid 1902 --cosmicray --debug -s "$var" -e "$var" --resubmission --aggregate 10
done

