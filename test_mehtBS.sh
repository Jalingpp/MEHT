if [ -z $1 ]; then
    round=1
else
    round=$1
fi
siMode=meht
U4=Synthesis_U4
U4Num=2000000
thread=32
batch_size=1000
mehtBS=(0 1 2 3 4)
mbtBN=9000
insertion_and_query_file="test_insertion_and_query.go"
mehtBC=500

for BS in ${mehtBS[*]}; do
    for ((i=0;i<$round;++i)); do
        go run $insertion_and_query_file $siMode $U4Num $thread $batch_size $mbtBN $mehtBC $BS $U4
        sleep 1
        rm -rf data/levelDB
        sleep 1
    done
done