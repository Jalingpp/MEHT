go run test_insertion_and_query.go mbt 1000000 128 1000 9000 500 1 nft-trans-100W
sleep 1
du -sh data/levelDB/PrimaryDB1000000mbt >> data/Real1dbsizePrimarymbt
sleep 1
du -sh data/levelDB/SecondaryDB1000000mbt >> data/Real1dbsizeSecondarymbt
sleep 1
rm -rf data/levelDB
sleep 1

go run test_insertion_and_query.go mbt 1500000 128 1000 9000 500 1 nft-trans-150W
sleep 1
du -sh data/levelDB/PrimaryDB1500000mbt >> data/Real2dbsizePrimarymbt
sleep 1
du -sh data/levelDB/SecondaryDB1500000mbt >> data/Real2dbsizeSecondarymbt
sleep 1
rm -rf data/levelDB
sleep 1

go run test_insertion_and_query.go mbt 2000000 128 1000 9000 500 1 nft-trans-200W
sleep 1
du -sh data/levelDB/PrimaryDB2000000mbt >> data/Real3dbsizePrimarymbt
sleep 1
du -sh data/levelDB/SecondaryDB2000000mbt >> data/Real3dbsizeSecondarymbt
sleep 1
rm -rf data/levelDB
sleep 1

go run test_insertion_and_query.go mbt 2500000 127 1000 9000 500 1 nft-trans-250W
sleep 1
du -sh data/levelDB/PrimaryDB2500000mbt >> data/Real4dbsizePrimarymbt
sleep 1
du -sh data/levelDB/SecondaryDB2500000mbt >> data/Rea41dbsizeSecondarymbt
sleep 1
rm -rf data/levelDB
sleep 1

go run test_insertion_and_query.go mbt 3000000 127 1000 9000 500 1 nft-trans-300W
sleep 1
du -sh data/levelDB/PrimaryDB3000000mbt >> data/Real5dbsizePrimarymbt
sleep 1
du -sh data/levelDB/SecondaryDB3000000mbt >> data/Real5dbsizeSecondarymbt
sleep 1
rm -rf data/levelDB
sleep 1

go run test_insertion_and_query.go mpt 1000000 128 1000 9000 500 1 nft-trans-100W
sleep 1
du -sh data/levelDB/PrimaryDB1000000mpt >> data/Real1dbsizePrimarympt
sleep 1
du -sh data/levelDB/SecondaryDB1000000mpt >> data/Real1dbsizeSecondarympt
sleep 1
rm -rf data/levelDB
sleep 1

go run test_insertion_and_query.go mpt 1500000 128 1000 9000 500 1 nft-trans-150W
sleep 1
du -sh data/levelDB/PrimaryDB1500000mpt >> data/Real2dbsizePrimarympt
sleep 1
du -sh data/levelDB/SecondaryDB1500000mpt >> data/Real2dbsizeSecondarympt
sleep 1
rm -rf data/levelDB
sleep 1

go run test_insertion_and_query.go mpt 2000000 128 1000 9000 500 1 nft-trans-200W
sleep 1
du -sh data/levelDB/PrimaryDB2000000mpt >> data/Real3dbsizePrimarympt
sleep 1
du -sh data/levelDB/SecondaryDB2000000mpt >> data/Real3dbsizeSecondarympt
sleep 1
rm -rf data/levelDB
sleep 1

go run test_insertion_and_query.go mpt 2500000 127 1000 9000 500 1 nft-trans-250W
sleep 1
du -sh data/levelDB/PrimaryDB2500000mpt >> data/Real4dbsizePrimarympt
sleep 1
du -sh data/levelDB/SecondaryDB2500000mpt >> data/Rea41dbsizeSecondarympt
sleep 1
rm -rf data/levelDB
sleep 1

go run test_insertion_and_query.go mpt 3000000 127 1000 9000 500 1 nft-trans-300W
sleep 1
du -sh data/levelDB/PrimaryDB3000000mpt >> data/Real5dbsizePrimarympt
sleep 1
du -sh data/levelDB/SecondaryDB3000000mpt >> data/Real5dbsizeSecondarympt
sleep 1
rm -rf data/levelDB
sleep 1

go run test_insertion_and_query.go meht 1000000 128 1000 9000 500 1 nft-trans-100W
sleep 1
du -sh data/levelDB/PrimaryDB1000000meht >> data/Real1dbsizePrimarymeht
sleep 1
du -sh data/levelDB/SecondaryDB1000000meht >> data/Real1dbsizeSecondarymeht
sleep 1
rm -rf data/levelDB
sleep 1

go run test_insertion_and_query.go meht 1500000 128 1000 9000 500 1 nft-trans-150W
sleep 1
du -sh data/levelDB/PrimaryDB1500000meht >> data/Real2dbsizePrimarymeht
sleep 1
du -sh data/levelDB/SecondaryDB1500000meht >> data/Real2dbsizeSecondarymeht
sleep 1
rm -rf data/levelDB
sleep 1

go run test_insertion_and_query.go meht 2000000 128 1000 9000 500 1 nft-trans-200W
sleep 1
du -sh data/levelDB/PrimaryDB2000000meht >> data/Real3dbsizePrimarymeht
sleep 1
du -sh data/levelDB/SecondaryDB2000000meht >> data/Real3dbsizeSecondarymeht
sleep 1
rm -rf data/levelDB
sleep 1

go run test_insertion_and_query.go meht 2500000 127 1000 9000 500 1 nft-trans-250W
sleep 1
du -sh data/levelDB/PrimaryDB2500000meht >> data/Real4dbsizePrimarymeht
sleep 1
du -sh data/levelDB/SecondaryDB2500000meht >> data/Real4dbsizeSecondarymeht
sleep 1
rm -rf data/levelDB
sleep 1

go run test_insertion_and_query.go meht 3000000 128 1000 9000 500 1 nft-trans-300W
sleep 1
du -sh data/levelDB/PrimaryDB3000000meht >> data/Real5dbsizePrimarymeht
sleep 1
du -sh data/levelDB/SecondaryDB3000000meht >> data/Real5dbsizeSecondarymeht
sleep 1
rm -rf data/levelDB
sleep 1