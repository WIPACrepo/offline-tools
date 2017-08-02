
for var in "$@"
do
     python MainSubmit_L3.py --cosmicray --sourcedatasetid 1862 --destinationdatasetid 1930 --aggregate 10 -s "$var" -e "$var" --ignoreL2validation
#     python MainSubmit_L3.py --sourcedatasetid 1874 --destinationdatasetid 1906 --cosmicray -s "$var" -e "$var" --resubmission --aggregate 10 --ignoreL2validation
done

