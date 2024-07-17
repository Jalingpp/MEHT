if [ -z $1 ]; then
    round=1
else
    round=$1
fi

bash test_batchSize.sh $round
sleep 1
bash test_mehtBC.sh $round
sleep 1
bash test_mehtBS.sh $round
sleep 1
bash test_mbtBN.sh $round
sleep 1
bash test_thread.sh $round
sleep 1
bash test_scale.sh $round
sleep 1
bash test_cache.sh $round
sleep 1
# bash test_real.sh $round
# sleep 1
