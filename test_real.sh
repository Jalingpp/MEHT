if [ -z $1 ]; then
    round=1
else
    round=$1
fi
siModes=(meht mbt mpt)
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

for siMode in ${siModes[*]}; do
    for ((i=0;i<$round;++i)); do
        go run $insertion_and_query_file $siMode $RealNum1 $thread $batch_size $mbtBN $mehtBC $mehtBS $Real1
        sleep 1
        du -sh data/levelDB/PrimaryDB$RealNum1$siMode >> data/Real1dbsizePrimary$siMode
        du -sh data/levelDB/SecondaryDB$RealNum1$siMode >> data/Real1dbsizeSecondary$siMode
        rm -rf data/levelDB
        sleep 1

        go run $insertion_and_query_file $siMode $RealNum2 $thread $batch_size $mbtBN $mehtBC $mehtBS $Real2
        sleep 1
        du -sh data/levelDB/PrimaryDB$RealNum2$siMode >> data/Real2dbsizePrimary$siMode
        du -sh data/levelDB/SecondaryDB$RealNum2$siMode >> data/Real2dbsizeSecondary$siMode
        rm -rf data/levelDB
        sleep 1

        go run $insertion_and_query_file $siMode $RealNum3 $thread $batch_size $mbtBN $mehtBC $mehtBS $Real3
        sleep 1
        du -sh data/levelDB/PrimaryDB$RealNum3$siMode >> data/Real3dbsizePrimary$siMode
        du -sh data/levelDB/SecondaryDB$RealNum3$siMode >> data/Real3dbsizeSecondary$siMode
        rm -rf data/levelDB
        sleep 1

        go run $insertion_and_query_file $siMode $RealNum4 $thread $batch_size $mbtBN $mehtBC $mehtBS $Real4
        sleep 1
        du -sh data/levelDB/PrimaryDB$RealNum4$siMode >> data/Real4dbsizePrimary$siMode
        du -sh data/levelDB/SecondaryDB$RealNum4$siMode >> data/Real4dbsizeSecondary$siMode
        rm -rf data/levelDB
        sleep 1

        go run $insertion_and_query_file $siMode $RealNum5 $thread $batch_size $mbtBN $mehtBC $mehtBS $Real5
        sleep 1
        du -sh data/levelDB/PrimaryDB$RealNum5$siMode >> data/Real5dbsizePrimary$siMode
        du -sh data/levelDB/SecondaryDB$RealNum5$siMode >> data/Real5dbsizeSecondary$siMode
        rm -rf data/levelDB
        sleep 1
    done
done