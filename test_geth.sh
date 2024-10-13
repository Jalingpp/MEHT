if [ -z $1 ]; then
    round=1
else
    round=$1
fi

#geth实验步骤：
#1.安装并启动geth(在go-ethereum 1.9.20 基础上修改重编译得到)：
#  cp ./geth /usr/local/bin/
#  cd ethnodes
#  geth --datadir ./ init ./genesis.json
#  geth --networkid 200 --datadir "./" --nodiscover --rpcapi personal console
#2.运行实验脚本
#  loadScript("run.js")
#5.等待/root/ethnodes/datafile文件中有数据后在新终端处执行本脚本：
#  ./test_geth.sh

# siModes=(mbt meht mpt)
siMode=mpt
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

# for siMode in ${siModes[*]}; do
    for ((i=0;i<$round;++i)); do
        go run $go_geth_file $siMode $RealNum $thread $batch_size $mbtBN $mehtBC $mehtBS $Real $mehtWs $mehtSt $mehtBFsize $mehtBFhnum $isBF
        sleep 1
        du -sh data/levelDB/PrimaryDB$RealNum$siMode >> data/RealdbsizePrimary$siMode
        du -sh data/levelDB/SecondaryDB$RealNum$siMode >> data/RealdbsizeSecondary$siMode
        rm -rf data/levelDB
        sleep 1
    done
# done