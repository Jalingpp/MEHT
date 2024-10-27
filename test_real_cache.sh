if [ -z $1 ]; then
    round=1
else
    round=$1
fi
# siModes=(meht mbt mpt)
siMode=meht
Real1=nft-trans-100W
RealNum1=1000000
Real2=nft-trans-150W
RealNum2=1500000
Real3=nft-trans-200W
RealNum3=2000000
Real4=nft-trans-250W
RealNum4=2500000
Real5=nft-trans-300W
RealNum5=3000000
thread=128
batch_size=1000
mehtBS=1
mbtBN=9000
mehtBC=500
insertion_and_query_file="test_insertion_and_query.go"
insertion_and_query_file_cache="test_insertion_and_query_cache.go"

isBF=false
mehtWs=4
mehtSt=2
mehtBFsize=400000
mehtBFhnum=3

# for siMode in ${siModes[*]}; do
    for ((i=0;i<$round;++i)); do
        go run $insertion_and_query_file $siMode $RealNum1 $thread $batch_size $mbtBN $mehtBC $mehtBS $Real1 $mehtWs $mehtSt $mehtBFsize $mehtBFhnum $isBF
        sleep 1
        rm -rf data/levelDB
        sleep 1
        go run $insertion_and_query_file_cache $siMode $RealNum1 $thread $batch_size $mbtBN $mehtBC $mehtBS $Real1 $mehtWs $mehtSt $mehtBFsize $mehtBFhnum $isBF
        sleep 1
        rm -rf data/levelDB
        sleep 1

        go run $insertion_and_query_file $siMode $RealNum2 $thread $batch_size $mbtBN $mehtBC $mehtBS $Real2 $mehtWs $mehtSt $mehtBFsize $mehtBFhnum $isBF
        sleep 1
        rm -rf data/levelDB
        sleep 2
        go run $insertion_and_query_file_cache $siMode $RealNum2 $thread $batch_size $mbtBN $mehtBC $mehtBS $Real2 $mehtWs $mehtSt $mehtBFsize $mehtBFhnum $isBF
        sleep 1
        rm -rf data/levelDB
        sleep 1

        go run $insertion_and_query_file $siMode $RealNum3 $thread $batch_size $mbtBN $mehtBC $mehtBS $Real3 $mehtWs $mehtSt $mehtBFsize $mehtBFhnum $isBF
        sleep 1
        rm -rf data/levelDB
        sleep 2
        go run $insertion_and_query_file_cache $siMode $RealNum3 $thread $batch_size $mbtBN $mehtBC $mehtBS $Real3 $mehtWs $mehtSt $mehtBFsize $mehtBFhnum $isBF
        sleep 1
        rm -rf data/levelDB
        sleep 1

        go run $insertion_and_query_file $siMode $RealNum4 $thread $batch_size $mbtBN $mehtBC $mehtBS $Real4 $mehtWs $mehtSt $mehtBFsize $mehtBFhnum $isBF
        sleep 1
        rm -rf data/levelDB
        sleep 2
        go run $insertion_and_query_file_cache $siMode $RealNum4 $thread $batch_size $mbtBN $mehtBC $mehtBS $Real4 $mehtWs $mehtSt $mehtBFsize $mehtBFhnum $isBF
        sleep 1
        rm -rf data/levelDB
        sleep 1

        go run $insertion_and_query_file $siMode $RealNum5 $thread $batch_size $mbtBN $mehtBC $mehtBS $Real5 $mehtWs $mehtSt $mehtBFsize $mehtBFhnum $isBF
        sleep 1
        rm -rf data/levelDB
        sleep 1
        go run $insertion_and_query_file_cache $siMode $RealNum5 $thread $batch_size $mbtBN $mehtBC $mehtBS $Real5 $mehtWs $mehtSt $mehtBFsize $mehtBFhnum $isBF
        sleep 1
        rm -rf data/levelDB
        sleep 1
    done
# done