if [ -z $1 ]; then
    round=1
else
    round=$1
fi
siMode=meht
U1=Synthesis_U1
U1Num=1000000
S1=Synthesis_S1
S2=Synthesis_S2
S3=Synthesis_S3
S4=Synthesis_S4
S5=Synthesis_S5
thread=32
batch_size=1000
mehtBC=500
mehtBS=1
mbtBN=9000

query_cached_file="test_query_cached.go"
pure_insertion_file="test_insertion.go"

for ((i=0;i<$round;++i)); do
    go run $pure_insertion_file $siMode $U1Num $thread $batch_size $mbtBN $mehtBC $mehtBS $U1
    sleep 1
    go run $query_cached_file $siMode $U1Num $thread $mbtBN $mehtBC $mehtBS $S1
    sleep 1
    go run $query_cached_file $siMode $U1Num $thread $mbtBN $mehtBC $mehtBS $S1
    sleep 1
    rm -rf data/levelDB
    sleep 1

    go run $pure_insertion_file $siMode $U1Num $thread $batch_size $mbtBN $mehtBC $mehtBS $U1
    sleep 1
    go run $query_cached_file $siMode $U1Num $thread $mbtBN $mehtBC $mehtBS $S2
    sleep 1
    go run $query_cached_file $siMode $U1Num $thread $mbtBN $mehtBC $mehtBS $S2
    sleep 1
    rm -rf data/levelDB
    sleep 1

    go run $pure_insertion_file $siMode $U1Num $thread $batch_size $mbtBN $mehtBC $mehtBS $U1
    sleep 1
    go run $query_cached_file $siMode $U1Num $thread $mbtBN $mehtBC $mehtBS $S3
    sleep 1
    go run $query_cached_file $siMode $U1Num $thread $mbtBN $mehtBC $mehtBS $S3
    sleep 1
    rm -rf data/levelDB
    sleep 1

    go run $pure_insertion_file $siMode $U1Num $thread $batch_size $mbtBN $mehtBC $mehtBS $U1
    sleep 1
    go run $query_cached_file $siMode $U1Num $thread $mbtBN $mehtBC $mehtBS $S4
    sleep 1
    go run $query_cached_file $siMode $U1Num $thread $mbtBN $mehtBC $mehtBS $S4
    sleep 1
    rm -rf data/levelDB
    sleep 1

    go run $pure_insertion_file $siMode $U1Num $thread $batch_size $mbtBN $mehtBC $mehtBS $U1
    sleep 1
    go run $query_cached_file $siMode $U1Num $thread $mbtBN $mehtBC $mehtBS $S5
    sleep 1
    go run $query_cached_file $siMode $U1Num $thread $mbtBN $mehtBC $mehtBS $S5
    sleep 1
    rm -rf data/levelDB
    sleep 1
done