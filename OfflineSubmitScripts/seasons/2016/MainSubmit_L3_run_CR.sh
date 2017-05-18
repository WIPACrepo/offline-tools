
for var in "$@"
do
     python MainSubmit_L3.py --sourcedatasetid 1874 --destinationdatasetid 1906 --cosmicray -s "$var" -e "$var" --resubmission --aggregate 10 --ignoreL2validation
done

