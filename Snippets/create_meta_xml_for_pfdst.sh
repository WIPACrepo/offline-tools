
for var in "$@"
do
    newvar="${var/Run0000/Run00}"
    myfile="${newvar/.i3.bz2/}"
    echo "var = $var"
    echo "newvar = $newvar"
    echo "myfile = $myfile"

    mv "$var" "$newvar"

    python /home/joertlin/workspace/Sandbox/Snippets/create_meta_xml_for_pfdst.py --output-folder /data/exp/IceCube/2015/unbiased/PFDST/0106/ --file "$newvar"
    bunzip2 "$newvar"
    tar -acf "$myfile"{.tar.gz,.i3,.meta.xml}
done

