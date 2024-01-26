siModes=(mbt meht mpt)
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
U7=(Synthesis_U7_100W Synthesis_U7_150W Synthesis_U7_200W Synthesis_U7_250W Synthesis_U7_300W)
U7Num=(1000000 1500000 2000000 2500000 3000000)
U8Insert=(Synthesis_U8_Insert100W Synthesis_U8_Insert150W Synthesis_U8_Insert200W Synthesis_U8_Insert250W Synthesis_U8_Insert300W)
U8Query=(Synthesis_U8_QueryOn100W Synthesis_U8_QueryOn150W Synthesis_U8_QueryOn200W Synthesis_U8_QueryOn250W Synthesis_U8_QueryOn300W)
U8Num=(1000000 1500000 2000000 2500000 3000000)
S1=(Synthesis_S1_10p Synthesis_S1_20p Synthesis_S1_30p Synthesis_S1_40p Synthesis_S1_50p)
S1Num=1000000
S2=(Synthesis_S2_10p Synthesis_S2_20p Synthesis_S2_30p Synthesis_S2_40p Synthesis_S2_50p)
S2Num=1000000
thread_option=(1 2 4 8 16 32)
batch_size=1000
# mehtBC=1000
mehtBS=1
mbtBN=1000
screw=(0.1 0.2 0.3 0.4 0.5)
pure_insertion_file="test_insertion.go"
pure_query_file="test_query.go"
insertion_and_query_file="test_insertion_and_query3.go"

siMode=meht
mehtBC=(500 1000 1500 2000 2500)
for BC in ${mehtBC[*]}; do 
    echo "go run $insertion_and_query_file $siMode $U4Num 32 $batch_size $mbtBN $BC $mehtBS $U4"
    echo "rm -rf data/levelDB"
done

for BC in ${mehtBC[*]}; do 
    for file in ${S2[*]}; do
        echo "go run $pure_insertion_file $siMode $U1Num 32 $batch_size $mbtBN $BC $mehtBS $U1"
        echo "go run $insertion_and_query_file $siMode $S2Num 32 $batch_size $mbtBN $BC $mehtBS $file"
        echo "rm -rf data/levelDB"
    done
done

# for siMode in ${siModes[*]};do
#     #test1
#     for thread in ${thread_option[*]};do
#         echo "go run $pure_insertion_file $siMode $U1Num $thread $batch_size $mbtBN $mehtBC $mehtBS $U1"
#         if [ $thread  -eq 32 ]; then 
#             echo "go run $pure_query_file $siMode $U3Num $thread $mbtBN $mehtBC $mehtBS $U3"
#             echo "rm -rf data/levelDB"
#             echo "go run $insertion_and_query_file $siMode $U4Num $thread $batch_size $mbtBN $mehtBC $mehtBS $U4"
#             echo "rm -rf data/levelDB"
#             echo "go run $insertion_and_query_file $siMode $U5Num $thread $batch_size $mbtBN $mehtBC $mehtBS $U5"
#             echo "rm -rf data/levelDB"
#             echo "go run $insertion_and_query_file $siMode $U6Num $thread $batch_size $mbtBN $mehtBC $mehtBS $U6"
#             echo "rm -rf data/levelDB"
#         else 
#             echo "rm -rf data/levelDB"
#         fi
#         echo "go run $insertion_and_query_file $siMode $RealNum $thread $batch_size $mbtBN $mehtBC $mehtBS $Real"
#         echo "rm -rf data/levelDB"
#     done
#     #-------------------------------------------------------------------------------------------------------#
#     #test2 & test4
#     for ((i=0;i<${#U7[@]};++i)); do
#         echo "go run $pure_insertion_file $siMode ${U7Num[${i}]} 32 $batch_size $mbtBN $mehtBC $mehtBS ${U7[${i}]}"
#         echo "rm -rf data/levelDB"
#     done
#     for ((i=0;i<${#U8Insert[@]};++i)); do
#         echo "go run $pure_insertion_file $siMode ${U8Num[${i}]} 32 $batch_size $mbtBN $mehtBC $mehtBS ${U8Insert[${i}]}"
#         echo "go run $pure_query_file $siMode ${U8Num[${i}]} 32 $mbtBN $mehtBC $mehtBS ${U8Query[${i}]}"
#         echo "du -sh data/levelDB/PrimaryDB${U8Num[${i}]}$siMode >> dbSize.txt"
#         echo "du -sh data/levelDB/SecondaryDB${U8Num[${i}]}$siMode >> dbSize.txt"
#         echo "rm -rf data/levelDB"
#     done
#     #-------------------------------------------------------------------------------------------------------#
#     #test3
#     for ((i=0;i<${#S1[@]};++i)); do
#         for screw_option in ${screw[*]}; do
#             echo "go run $pure_insertion_file $siMode $U1Num 32 $batch_size $mbtBN $mehtBC $mehtBS $U1"
#             echo "go run $pure_query_file $siMode $S1Num 32 $mbtBN $mehtBC $mehtBS ${S1[${i}]} $screw_option"
#             echo "rm -rf data/levelDB"
#         done
#     done
#     for ((i=0;i<${#S2[@]};++i)); do
#         for screw_option in ${screw[*]}; do
#             echo "go run $pure_insertion_file $siMode $U1Num 32 $batch_size $mbtBN $mehtBC $mehtBS $U1"
#             echo "go run $insertion_and_query_file $siMode $S2Num 32 $mbtBN $mehtBC $mehtBS ${S2[${i}]} $screw_option"
#             echo "rm -rf data/levelDB"
#         done
#     done
# done