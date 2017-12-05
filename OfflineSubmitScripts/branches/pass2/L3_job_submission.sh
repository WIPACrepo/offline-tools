
echo "##############################################"
echo "# Muon Run Submission                        #"
echo "##############################################"

# Muon
python MainSubmit_L3.py --sourcedatasetid 1921 --destinationdatasetid 1946 -s 127951 -e 128753
python MainSubmit_L3.py --sourcedatasetid 1920 --destinationdatasetid 1945 -s 126289 -e 127949
python MainSubmit_L3.py --sourcedatasetid 1919 --destinationdatasetid 1944 -s 124550 -e 126377
python MainSubmit_L3.py --sourcedatasetid 1918 --destinationdatasetid 1943 -s 122205 -e 124699
python MainSubmit_L3.py --sourcedatasetid 1917 --destinationdatasetid 1942 -s 120028 -e 122275
python MainSubmit_L3.py --sourcedatasetid 1916 --destinationdatasetid 1941 -s 118175 -e 120155
python MainSubmit_L3.py --sourcedatasetid 1922 --destinationdatasetid 1940 -s 115986 -e 118173

echo "##############################################"
echo "# Cascade Run Submission                     #"
echo "##############################################"

# Cascade
python MainSubmit_L3.py --sourcedatasetid 1921 --destinationdatasetid 1936 -s 127951 -e 128753
python MainSubmit_L3.py --sourcedatasetid 1920 --destinationdatasetid 1935 -s 126289 -e 127949
python MainSubmit_L3.py --sourcedatasetid 1919 --destinationdatasetid 1934 -s 124550 -e 126377
python MainSubmit_L3.py --sourcedatasetid 1918 --destinationdatasetid 1933 -s 122205 -e 124699
python MainSubmit_L3.py --sourcedatasetid 1917 --destinationdatasetid 1932 -s 120028 -e 122275
python MainSubmit_L3.py --sourcedatasetid 1916 --destinationdatasetid 1931 -s 118175 -e 120155
python MainSubmit_L3.py --sourcedatasetid 1922 --destinationdatasetid 1937 -s 115986 -e 118173

echo "##############################################"
echo "# LoweEnergy Run Submission                  #"
echo "##############################################"

# Muon
python MainSubmit_L3.py --sourcedatasetid 1921 --destinationdatasetid 1954 -s 127951 -e 128753 --aggregate 10
python MainSubmit_L3.py --sourcedatasetid 1920 --destinationdatasetid 1953 -s 126289 -e 127949 --aggregate 10
python MainSubmit_L3.py --sourcedatasetid 1919 --destinationdatasetid 1952 -s 124550 -e 126377 --aggregate 10
python MainSubmit_L3.py --sourcedatasetid 1918 --destinationdatasetid 1951 -s 122205 -e 124699 --aggregate 10
python MainSubmit_L3.py --sourcedatasetid 1917 --destinationdatasetid 1950 -s 120028 -e 122275 --aggregate 10
python MainSubmit_L3.py --sourcedatasetid 1916 --destinationdatasetid 1949 -s 118175 -e 120155 --aggregate 10

echo "##############################################"
echo "# Done :)                                    #"
echo "##############################################"
