【2023-12-10 16:22】
在hotaware分支中对由master分支copy来的代码进行了如下修复：

1.MGT.go
①MGTNode的中间节点的datahashes数量加了严格的rdx数量限定，体现在反序列化时和在NewMGTNode时。
②GetLeafNodeAndPath中修订了将p插入到result的第0位置，之前的写法是错的。
③MGTUpdate中更新叶子节点的dataHashes时，采用make函数创建而非nil。
④修订了ComputMGTRootHash函数中根据MGTProof计算MGT根哈希,之前的写法错误.

2.针对Bucket.go
问题：当bucket发生分裂后，在完成原桶中记录的再分配后，在插入待插数据到一个桶中时，需判断是否会引
发递归分裂，如果未引发，则可直接插入相应桶中，否则不可插入，因为此时分裂的桶还未在ht和mgt中
更新，继续分裂将难以追踪更新。
解决方案：如果需要继续分裂，则暂时不插入，添加一个布尔变量isInserted作为返回值，到MEHT.go中，先
完成当前层桶分裂造成的HT扩容和MGT更新后再插入，重复直到isInserted为真。

3.针对MEHT.go
为解决2中问题在insert函数中做了相应修改。