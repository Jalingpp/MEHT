if [ -z $1 ]; then
    round=1
else
    round=$1
fi
siModes=(mbt meht mpt)
U4=Synthesis_U4
U4Num=2000000
thread=32
batch_size=(1 100 1000 10000 100000)
mehtBS=1
mbtBN=9000
insertion_and_query_file="test_insertion_and_query.go"
mehtBC=500

for siMode in ${siModes[*]}; do
    for batch in ${batch_size[*]}; do
        for ((i=0;i<$round;++i)); do
            go run $insertion_and_query_file $siMode $U4Num $thread $batch $mbtBN $mehtBC $mehtBS $U4
            sleep 1
            rm -rf data/levelDB
            sleep 1
        done
    done
done