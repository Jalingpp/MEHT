if [ -z $1 ]; then
    round=1
else
    round=$1
fi
siMode=meht
# Reals=(nft-trans-100W nft-trans-150W nft-trans-200W nft-trans-250W nft-trans-300W)
# RealNums=(1000000 1500000 2000000 2500000 3000000)
# datasetNum=5
Reals=(Synthesis_BSFG_4 Synthesis_BSFG_8 Synthesis_BSFG_12 Synthesis_BSFG_16 Synthesis_BSFG_20)
RealNums=(1000000 1000000 1000000 1000000 1000000)
datasetNum=5
# Real=Synthesis_BSFG_20
# RealNum=1000000
thread=32
batch=1000
mehtBS=1
mbtBN=9000
insertion_and_query_file="test_insertion_and_query.go"
mehtBC=500
isBFT=true
isBFF=false
# mehtWs=(1 2 3 4 5 6 7 8)
# mehtSt=4
ws=4
mehtSts=(1 2 3 4)
mehtBFsize=100000
mehtBFhnum=3


for ((i=0;i<$round;++i)); do
    for (( i=0; i<$datasetNum; i++ ))
    do
        RealNum=${RealNums[$i]}
        Real=${Reals[$i]}
        # for ws in ${mehtWs[*]}; do
        for st in ${mehtSts[*]}; do
            go run $insertion_and_query_file $siMode $RealNum $thread $batch $mbtBN $mehtBC $mehtBS $Real $ws $st $mehtBFsize $mehtBFhnum $isBFF
            sleep 1
            du -sh data/levelDB/SecondaryDB$RealNum$siMode >> "data/dbsizeSecondaryBSFG$isBFF"
            rm -rf data/levelDB
            sleep 1
            go run $insertion_and_query_file $siMode $RealNum $thread $batch $mbtBN $mehtBC $mehtBS $Real $ws $st $mehtBFsize $mehtBFhnum $isBFT
            sleep 1
            du -sh data/levelDB/SecondaryDB$RealNum$siMode >> "data/dbsizeSecondaryBSFG$isBFT"
            rm -rf data/levelDB
            sleep 1
        done
    done
done
