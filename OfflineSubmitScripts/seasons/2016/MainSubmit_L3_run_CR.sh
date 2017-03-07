
for var in "$@"
do
     python MainSubmit_L3.py --sourcedatasetid 1883 --destinationdatasetid 1908 --cosmicray --debug -s "$var" -e "$var" --resubmission --aggregate 10 --ignoreL2validation
done

