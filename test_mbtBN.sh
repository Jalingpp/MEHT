if [ -z $1 ]; then
    round=1
else
    round=$1
fi
siMode=mbt
U4=Synthesis_U4
U4Num=2000000
thread=32
batch_size=1000
mehtBS=1
# mbtBN=(8000 8500 9000 9500 10000)
insertion_and_query_file="test_insertion_and_query.go"
mehtBC=500

for ((BN=1000;BN<=50000;BN+=1000)) do
    for ((i=0;i<$round;++i)); do
        go run $insertion_and_query_file $siMode $U4Num $thread $batch_size $BN $mehtBC $mehtBS $U4
        sleep 1
        rm -rf data/levelDB
        sleep 1
    done
done