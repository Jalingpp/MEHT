if [ -z $1 ]; then
    round=1
else
    round=$1
fi
siMode=meht
U1=Synthesis_U1
U1Num=1000000
U3=Synthesis_U3
U3Num=1000000
thread=32
batch_size=1000
mehtBS=(0 1 2 3 4)
mbtBN=9000
insertion_file="test_insertion.go"
query_file="test_query.go"
mehtBC=500

for BS in ${mehtBS[*]}; do
    for ((i=0;i<$round;++i)); do
        go run $insertion_file $siMode $U1Num $thread $batch_size $mbtBN $mehtBC $BS $U1
        go run $query_file $siMode $U3Num $thread $mbtBN $mehtBC $BS $U3
        sleep 1
        rm -rf data/levelDB
        sleep 1
    done
done