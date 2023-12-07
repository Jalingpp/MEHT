package mbt

import (
	"MEHT/util"
	"fmt"
)

type ProofElement struct {
	level          int      // level of proof
	proofType      int      // 0:Leaf node, 2:branch node
	value          []byte   // value of leaf node or branch node
	nextNodeHash   []byte   // hash of next node
	childrenHashes [][]byte // children hashes of branch node
}

func NewProofElement(level int, proofType int, value []byte, nextNodeHash []byte, childrenHash [][]byte) *ProofElement {
	return &ProofElement{level, proofType, value, nextNodeHash, childrenHash}
}

func (proofElement *ProofElement) GetSizeOf() uint {
	ret := 2*util.SIZEOFINT + uint(len(proofElement.value)+len(proofElement.nextNodeHash))*util.SIZEOFBYTE
	for _, hash := range proofElement.childrenHashes {
		ret += uint(len(hash)) * util.SIZEOFBYTE
	}
	return ret
}

func (proofElement *ProofElement) PrintProofElement() {
	if proofElement.proofType == 0 {
		fmt.Println("level=", proofElement.level, ", proofType=leaf node, value=", proofElement.value, ".")
		return
	} else {
		fmt.Println("level=", proofElement.level, ", proofType=branch node, value=", proofElement.value, ".")
		for i, hash := range proofElement.childrenHashes {
			if hash != nil {
				fmt.Println("[", i, "]", hash)
			}
		}
	}
}

type MBTProof struct {
	isExist bool
	proofs  []*ProofElement
}

func (mbtProof *MBTProof) GetSizeOf() uint {
	ret := util.SIZEOFBOOL
	for _, proof := range mbtProof.proofs {
		ret += proof.GetSizeOf()
	}
	return ret
}

func (mbtProof *MBTProof) GetExist() bool {
	return mbtProof.isExist
}

func (mbtProof *MBTProof) PrintMBTProof() {
	fmt.Printf("打印MBTProof-------------------------------------------------------------------------------------------\n")
	fmt.Printf("isExist=", mbtProof.isExist)
	for _, proof := range mbtProof.proofs {
		proof.PrintProofElement()
	}
}
