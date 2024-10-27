if [ -z $1 ]; then
    round=1
else
    round=$1
fi
siModes=(mpt mbt meht)
U1=Synthesis_U1
U1Num=1000000
U2=Synthesis_U2
U2Num=2000000
U3=Synthesis_U3
U3Num=1000000
U4=Synthesis_U4
U4Num=2000000
U5=Synthesis_U5
U5Num=2000000
U6=Synthesis_U6
U6Num=2000000
thread_option=(1 2 4 8 16 32)
batch_size=1000
mehtBC=500
mehtBS=1
mbtBN=9000
pure_insertion_file="test_insertion.go"
pure_query_file="test_query_wo_cache.go"
insertion_and_query_file="test_insertion_and_query.go"

for siMode in ${siModes[*]};do
    for ((i=0;i<$round;++i)); do
        for thread in ${thread_option[*]};do
            # go run $pure_insertion_file $siMode $U1Num $thread $batch_size $mbtBN $mehtBC $mehtBS $U1
            # sleep 1
            go run $pure_query_file $siMode $U3Num $thread $batch_size  $mbtBN $mehtBC $mehtBS $U3 $U1
            sleep 1
            rm -rf data/levelDB
            sleep 1
        done
    done
done