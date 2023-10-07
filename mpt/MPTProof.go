package mpt

import "fmt"

type ProofElement struct {
	level int // level of proof

	proofType int // 0:leaf node, 1: extension node, 2: branch node

	prefix         []byte     // prefix of leaf node or extension node
	suffix         []byte     // suffix of leaf node or extension node
	value          []byte     // value of leaf node or branch node
	nextNodeHash   []byte     // next node hash of extension node
	childrenHashes [16][]byte // children hashes of branch node
}

type MPTProof struct {
	isExist bool            //是否存在
	levels  int             //证明的最高level
	proofs  []*ProofElement //存在证明的elements
}

func NewProofElement(level int, proofType int, prefix []byte, suffix []byte, value []byte, nextNodeHash []byte, childrenHashes [16][]byte) *ProofElement {
	return &ProofElement{level, proofType, prefix, suffix, value, nextNodeHash, childrenHashes}
}

// GetProofs returns proofs
func (mptProof *MPTProof) GetProofs() []*ProofElement {
	return mptProof.proofs
}

// GetIsExist returns isExist
func (mptProof *MPTProof) GetIsExist() bool {
	return mptProof.isExist
}

// GetLevels returns levels
func (mptProof *MPTProof) GetLevels() int {
	return mptProof.levels
}

// PrintMPTProof prints MPTProof
func (mptProof *MPTProof) PrintMPTProof() {
	fmt.Printf("打印MPTProof-------------------------------------------------------------------------------------------\n")
	fmt.Printf("isExist=%t, levels=%d\n", mptProof.isExist, mptProof.levels)
	for i := 0; i < len(mptProof.proofs); i++ {
		mptProof.proofs[i].PrintProofElement()
	}
}

// PrintProofElement prints ProofElement
func (proofElement *ProofElement) PrintProofElement() {
	//如果是叶子节点
	if proofElement.proofType == 0 {
		fmt.Printf("level=%d, proofType=leaf node, prefix=%x, suffix=%x,value=%x\n", proofElement.level, proofElement.prefix, proofElement.suffix, proofElement.value)
		return
	} else if proofElement.proofType == 1 {
		//如果是扩展节点
		fmt.Printf("level=%d, proofType=extension node, prefix=%x suffix=%x, nextNodeHash=%x\n", proofElement.level, proofElement.prefix, proofElement.suffix, proofElement.nextNodeHash)
	} else {
		//如果是分支节点
		fmt.Printf("level=%d, proofType=branch node,value=%x\n", proofElement.level, proofElement.value)
		for i := 0; i < 16; i++ {
			if proofElement.childrenHashes[i] != nil {
				fmt.Printf("[%d]%x\n", i, proofElement.childrenHashes[i])
			}
		}
	}
}
