if [ -z $1 ]; then
    round=1
else
    round=$1
fi
siModes=(mpt)
U7=(Synthesis_U7_100W Synthesis_U7_150W Synthesis_U7_200W Synthesis_U7_250W Synthesis_U7_300W)
U7Num=(1000000 1500000 2000000 2500000 3000000)
U8Insert=(Synthesis_U8_Insert100W Synthesis_U8_Insert150W Synthesis_U8_Insert200W Synthesis_U8_Insert250W Synthesis_U8_Insert300W)
U8Query=(Synthesis_U8_QueryOn100W Synthesis_U8_QueryOn150W Synthesis_U8_QueryOn200W Synthesis_U8_QueryOn250W Synthesis_U8_QueryOn300W)
U8Num=(1000000 1500000 2000000 2500000 3000000)
thread_option=(1 2 4 8 16 32)
batch_size=1000
mehtBC=500
mehtBS=1
mbtBN=9000
pure_insertion_file="test_insertion.go"
pure_query_file="test_query.go"


for siMode in ${siModes[*]};do
    for ((j=0;j<$round;++j)); do
        for ((i=0;i<${#U7[@]};++i)); do
            go run $pure_insertion_file $siMode ${U7Num[${i}]} 32 $batch_size $mbtBN $mehtBC $mehtBS ${U7[${i}]}
            sleep 1
            du -sh data/levelDB/PrimaryDB${U7Num[${i}]}$siMode >> data/U7dbsizeU1Primary$i$siMode
            du -sh data/levelDB/SecondaryDB${U7Num[${i}]}$siMode >> data/U7dbsizeU1Secondary$i$siMode
            rm -rf data/levelDB
            sleep 1
        done

        for ((i=0;i<${#U8Insert[@]};++i)); do
            go run $pure_insertion_file $siMode ${U8Num[${i}]} 32 $batch_size $mbtBN $mehtBC $mehtBS ${U8Insert[${i}]}
            sleep 1
            go run $pure_query_file $siMode ${U8Num[${i}]} 32 $mbtBN $mehtBC $mehtBS ${U8Query[${i}]}
            rm -rf data/levelDB
            sleep 1
        done
    done
done