package mpt

import (
	"MEHT/util"
	"fmt"
)

type ProofElement struct {
	level          int        // level of proof
	proofType      int        // 0:leaf node, 1: extension node, 2: branch node
	prefix         string     // prefix of leaf node or extension node
	suffix         string     // suffix of leaf node or extension node
	value          []byte     // value of leaf node or branch node
	nextNodeHash   []byte     // next node hash of extension node
	childrenHashes [16][]byte // children hashes of branch node
}

// GetSizeOf 获取ProofElement大小
func (proofElement *ProofElement) GetSizeOf() uint {
	ret := 2*util.SIZEOFINT + uint(len(proofElement.prefix)+len(proofElement.suffix))*util.SIZEOFBYTE +
		uint(len(proofElement.value)+len(proofElement.nextNodeHash))*util.SIZEOFBYTE
	for _, hash := range proofElement.childrenHashes {
		ret += uint(len(hash)) * util.SIZEOFBYTE
	}
	return ret
}

type MPTProof struct {
	isExist bool            //是否存在
	levels  int             //证明的最高level
	proofs  []*ProofElement //存在证明的elements
}

// GetSizeOf 获取MPTProof大小
func (mptProof *MPTProof) GetSizeOf() uint {
	ret := util.SIZEOFBOOL + util.SIZEOFINT
	for _, proof := range mptProof.proofs {
		ret += proof.GetSizeOf()
	}
	return ret
}

func NewProofElement(level int, proofType int, prefix string, suffix string, value []byte, nextNodeHash []byte, childrenHashes [16][]byte) *ProofElement {
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

// PrintMPTProof 打印 MPTProof
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
