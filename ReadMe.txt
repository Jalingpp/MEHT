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

【2023-12-13 16:00】
实现了负载感知与缓存调整功能，在小规模数据（testdata.txt）上完成测试。具体的实现内容如下：

1.数据结构的变化
①MGTNode中添加了cachedNodes和cachedDataHashes用于存储缓存的节点及其哈希值。
②MGT中添加了hotnessList（统计各叶子节点的访问频次），cachedLNMap（记录当前被缓存的叶子节点，
cachedINMap（记录当前被缓存的中间节点），accessLength（统计叶子节点被访路径总长。）
③上述结构变化造成的初始化、nodeHash计算方式（包含cachedDataHashes）、序列化与反序列化、
GetFromDB函数等的添加与修改。

2.统计访问频次
①程序每次运行，访问频次表都从空开始统计，代码上体现为不将hotnessList和accessLength写入DB。
②在MGT.go中添加UpdateHotnessList函数，针对多种访问情况统一处理。该函数被在三处调用，分别是：
第1处，MGTUpdate函数中，当根节点为空时新建根叶子节点后，对根叶子节点的访问频次计1；
第2处，GetLeafNodeAndPath函数中，分别对访问的叶子节点访问频次计1，该函数会在insert时被调用
至少1-2次，在query时被调用至少1次；
第3处，MGTGrow函数中，对于分裂的叶子节点，新的叶子节点将分摊原节点的访问频次。
③在MGT.go中添加了打印hotnessList和总访问次数的函数，hotnessList按热度降序打印。

3.统计访问路径总长度
①访问路径长度均在GetLeafNodeAndPath函数中统计。
②考虑到叶子节点可能缓存在某个更靠近根节点的中间节点中，因此GetLeafNodeAndPath函数需要发生变
化。当待访叶子不在缓存Map中时，从根节点一路找subNodes；当带访叶子在缓存Map中时，从根节点开始
先找根节点的cachedNodes：如果找到一个cachedNode是叶子节点，则判断是否是待访叶子；如果找到的
一个cachedNode是非叶子，比较bucketKey是否是待访叶子的后缀，是则说明待访叶子是由一个缓存的中
间节点分裂而来，则递归此cachedNode的subNodes，不是后缀则说明待访叶子不在此缓存位置，再看根节
点的subNodes，以一个subNode为起点继续上述步骤。
【上述步骤实现了返回的路径是叶子节点的缓存路径，没有缓存时返回原始路径】

4.判断是否需要调整缓存
判断依据：当前缓存分布下访问路径总长大于阈值L，L根据桶的总数、冷热比例、总访问次数计算。
实现：在MGT.go中添加阈值L的计算函数并判断是否需要调整缓存，IsNeedCacheAdjust。

5.调整缓存分布
在MGT中添加CacheAdjust函数完成缓存调整功能。
①第一步：先将所有非叶子节点放回原处。缓存的叶子节点可能会发生分裂，因此标记并额外处理这类非叶节点。
为了找到非叶子节点的父节点，在MGT.go中添加GetInternalNodeAndPath函数。
②第二步：从hotnessList中依次选取最热的叶子节点进行放置。分为四种情况：
第1种，当前叶子节点的访问路径小于等于2时，表示该叶子节点是根节点或者在根节点的缓存目录中，
已经没有缓存优化的空间，直接返回。否则，根节点开始判断访问路径中的每个节点是否可以缓存当前
叶子节点。
第2种，节点的相应缓存为空，直接放置当前叶子节点。
第3种，节点的相应缓存为放了一个不如当前叶子节点热的节点，则用当前叶子替换。
第4种，节点的相应缓存位放了一个比当前叶子更热的节点，则继续探查下一个节点。
③第三步：调整完hotnessList中所有的叶子节点后，清空统计列表和访问路径总长。
④最后将mgt更新至DB并返回。
⑤在MEHT.go中添加MGTCachedAdjust函数，外部必须调用此函数而不能直接调用MGT.go中的CacheAdjust，
因为mgt更新后的根节点哈希需要告知MEHT，否则将难以从DB中找到mgt而报错。

【2023-12-15】10:15
修订了两处错误：
1.在MGT.go的IsNeedCacheAdjust函数中修订了缓存调整触发条件中阈值的计算公式，原来的是错误的。
2.在MGT。go的GetLeafNodeAndPath函数中修改了调用UpdateAccessLengthSum时的输入参数，即在统计
LN访问路径总长度时，不将根节点算在内，这是为了与阈值计算公式中对访问路径的计算一致，阈值计算公
式中对bucket总数取log得到的访问路径长度是不包含根节点的。