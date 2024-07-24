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
mehtBS=1
mbtBN=9000
insertion_and_query_file="test_insertion_and_query.go"
mehtBC=(250 500 750 1000 1250)

for ((i=0;i<$round;++i)); do
    for BC in ${mehtBC[*]}; do
        go run $insertion_and_query_file $siMode $U4Num $thread $batch_size $mbtBN $BC $mehtBS $U4
        rm -rf data/levelDB
        sleep 1
    done
done