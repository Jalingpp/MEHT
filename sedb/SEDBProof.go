package sedb

import (
	"MEHT/meht"
	"MEHT/mpt"
	"fmt"
)

type SEDBProof struct {
	primaryIndexProof       *mpt.MPTProof   //mpt类型的主索引的证明
	secondaryMPTIndexProof  *mpt.MPTProof   //mpt类型的辅助索引的证明
	secondaryMEHTIndexProof *meht.MEHTProof //meht类型的辅助索引的证明
}

// NewSEDBProof() *SEDBProof: 返回一个新的SEDBProof
func NewSEDBProof(pProof *mpt.MPTProof, sMPTProof *mpt.MPTProof, sMEHTProof *meht.MEHTProof) *SEDBProof {
	return &SEDBProof{pProof, sMPTProof, sMEHTProof}
}

// GetPrimaryIndexProof() *mpt.MPTProof: 返回primaryIndexProof
func (sedbProof *SEDBProof) GetPrimaryIndexProof() *mpt.MPTProof {
	return sedbProof.primaryIndexProof
}

// GetSecondaryMPTIndexProof() *mpt.MPTProof: 返回secondaryMPTIndexProof
func (sedbProof *SEDBProof) GetSecondaryMPTIndexProof() *mpt.MPTProof {
	return sedbProof.secondaryMPTIndexProof
}

// GetSecondaryMEHTIndexProof() *meht.MEHTProof: 返回secondaryMEHTIndexProof
func (sedbProof *SEDBProof) GetSecondaryMEHTIndexProof() *meht.MEHTProof {
	return sedbProof.secondaryMEHTIndexProof
}

// SetPrimaryIndexProof(proof *mpt.MPTProof) {} : 设置primaryIndexProof
func (sedbProof *SEDBProof) SetPrimaryIndexProof(proof *mpt.MPTProof) {
	sedbProof.primaryIndexProof = proof
}

// SetSecondaryMPTIndexProof(proof *mpt.MPTProof) {} : 设置secondaryMPTIndexProof
func (sedbProof *SEDBProof) SetSecondaryMPTIndexProof(proof *mpt.MPTProof) {
	sedbProof.secondaryMPTIndexProof = proof
}

// SetSecondaryMEHTIndexProof(proof *meht.MEHTProof) {} : 设置secondaryMEHTIndexProof
func (sedbProof *SEDBProof) SetSecondaryMEHTIndexProof(proof *meht.MEHTProof) {
	sedbProof.secondaryMEHTIndexProof = proof
}

// PrintSEDBProof() {} : 打印SEDBProof
func (sedbProof *SEDBProof) PrintSEDBProof() {
	fmt.Printf("打印SEDBProof---------------------------------------------------------------\n")
	//打印primaryIndexProof
	if sedbProof.primaryIndexProof == nil {
		fmt.Printf("primaryIndexProof为空\n")
	} else {
		sedbProof.primaryIndexProof.PrintMPTProof()
	}
	//打印secondaryMPTIndexProof
	if sedbProof.secondaryMPTIndexProof == nil {
		fmt.Printf("secondaryMPTIndexProof为空\n")
	} else {
		sedbProof.secondaryMPTIndexProof.PrintMPTProof()
	}
	//打印secondaryMEHTIndexProof
	if sedbProof.secondaryMEHTIndexProof == nil {
		fmt.Printf("secondaryMEHTIndexProof为空\n")
	} else {
		meht.PrintMEHTProof(sedbProof.secondaryMEHTIndexProof)
	}
}
