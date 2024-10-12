if [ -z $1 ]; then
    round=1
else
    round=$1
fi

#geth实验步骤：
#1.安装并启动geth：
#  cp ./geth /usr/local/bin/
#  cd ethnodes
#  geth --datadir ./ init ./genesis.json
#  geth --networkid 200 --datadir "./" --nodiscover --rpcapi personal console
#2.创建账户并启动挖矿获取账户0的启动资金：
#  personal.newAccount("123456")
#  personal.newAccount("123456")
#  miner.setEtherbase(eth.accounts[0])
#  miner.start()
#3.等待十秒左右后给账户1转启动资金：
#  loadScript("loadRawTxs.js")
#4.发送1000交易到交易池(实际实验发送100W)：
#  loadScript("loadTxs1000.js")
#5.等待/root/ethnodes/datafile文件中有数据（很久很久...）后在新终端处执行本脚本：
#  ./test_geth.sh

siModes=(meht mbt mpt)
Real=nft-trans-100W
RealNum=1000000
thread=128
batch_size=1000
mehtBS=1
mbtBN=9000
mehtBC=500
go_geth_file="test_geth.go"

isBF=true
mehtWs=4
mehtSt=2
mehtBFsize=400000
mehtBFhnum=3

for siMode in ${siModes[*]}; do
    for ((i=0;i<$round;++i)); do
        go run $go_geth_file $siMode $RealNum $thread $batch_size $mbtBN $mehtBC $mehtBS $Real $mehtWs $mehtSt $mehtBFsize $mehtBFhnum $isBF
        sleep 1
        du -sh data/levelDB/PrimaryDB$RealNum$siMode >> data/RealdbsizePrimary$siMode
        du -sh data/levelDB/SecondaryDB$RealNum$siMode >> data/RealdbsizeSecondary$siMode
        rm -rf data/levelDB
        sleep 1
    done
done