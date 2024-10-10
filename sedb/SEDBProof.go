package sedb

import (
	"fmt"

	"github.com/Jalingpp/MEST/mbt"
	"github.com/Jalingpp/MEST/meht"
	"github.com/Jalingpp/MEST/mpt"
)

type SEDBProof struct {
	primaryIndexProof       []*mpt.MPTProof //mpt类型的主索引的证明
	secondaryMPTIndexProof  *mpt.MPTProof   //mpt类型的辅助索引的证明
	secondaryMBTIndexProof  *mbt.MBTProof   //mbt类型的辅助索引的证明
	secondaryMEHTIndexProof *meht.MEHTProof //meht类型的辅助索引的证明
}

// GetSizeOf 获取当前SEDBProof大小
func (sedbProof *SEDBProof) GetSizeOf() (ret uint) {
	if sedbProof.secondaryMPTIndexProof != nil {
		ret += sedbProof.secondaryMPTIndexProof.GetSizeOf()
	} else if sedbProof.secondaryMEHTIndexProof != nil {
		ret += sedbProof.secondaryMEHTIndexProof.GetSizeOf()
	} else if sedbProof.secondaryMBTIndexProof != nil {
		ret += sedbProof.secondaryMBTIndexProof.GetSizeOf()
	}
	for _, proof := range sedbProof.primaryIndexProof {
		ret += proof.GetSizeOf()
	}
	return
}

// NewSEDBProof 返回一个新的SEDBProof
func NewSEDBProof(pProof []*mpt.MPTProof, sMPTProof *mpt.MPTProof, sMBTProof *mbt.MBTProof, sMEHTProof *meht.MEHTProof) *SEDBProof {
	return &SEDBProof{pProof, sMPTProof, sMBTProof, sMEHTProof}
}

// GetPrimaryIndexProof 返回primaryIndexProof
func (sedbProof *SEDBProof) GetPrimaryIndexProof() []*mpt.MPTProof {
	return sedbProof.primaryIndexProof
}

// GetSecondaryMPTIndexProof 返回secondaryMPTIndexProof
func (sedbProof *SEDBProof) GetSecondaryMPTIndexProof() *mpt.MPTProof {
	return sedbProof.secondaryMPTIndexProof
}

// GetSecondaryMBTIndexProof 返回secondaryMBTIndexProof
func (sedbProof *SEDBProof) GetSecondaryMBTIndexProof() *mbt.MBTProof {
	return sedbProof.secondaryMBTIndexProof
}

// GetSecondaryMEHTIndexProof 返回secondaryMEHTIndexProof
func (sedbProof *SEDBProof) GetSecondaryMEHTIndexProof() *meht.MEHTProof {
	return sedbProof.secondaryMEHTIndexProof
}

// SetPrimaryIndexProof 设置primaryIndexProof
func (sedbProof *SEDBProof) SetPrimaryIndexProof(proof []*mpt.MPTProof) {
	sedbProof.primaryIndexProof = proof
}

// SetSecondaryMPTIndexProof 设置secondaryMPTIndexProof
func (sedbProof *SEDBProof) SetSecondaryMPTIndexProof(proof *mpt.MPTProof) {
	sedbProof.secondaryMPTIndexProof = proof
}

// SetSecondaryMBTIndexProof 设置secondaryMBTIndexProof
func (sedbProof *SEDBProof) SetSecondaryMBTIndexProof(proof *mbt.MBTProof) {
	sedbProof.secondaryMBTIndexProof = proof
}

// SetSecondaryMEHTIndexProof 设置secondaryMEHTIndexProof
func (sedbProof *SEDBProof) SetSecondaryMEHTIndexProof(proof *meht.MEHTProof) {
	sedbProof.secondaryMEHTIndexProof = proof
}

// AddPrimaryProof 添加一个proof到primaryIndexProof中
func (sedbProof *SEDBProof) AddPrimaryProof(proof *mpt.MPTProof) {
	sedbProof.primaryIndexProof = append(sedbProof.primaryIndexProof, proof)
}

// PrintSEDBProof 打印SEDBProof
func (sedbProof *SEDBProof) PrintSEDBProof() {
	fmt.Printf("打印SEDBProof---------------------------------------------------------------\n")
	//打印primaryIndexProof
	if len(sedbProof.primaryIndexProof) == 0 {
		fmt.Printf("primaryIndexProof为空\n")
	} else {
		proofs := sedbProof.primaryIndexProof
		for _, proof := range proofs {
			proof.PrintMPTProof()
		}
	}
	//打印secondaryMPTIndexProof
	if sedbProof.secondaryMPTIndexProof == nil {
		fmt.Printf("secondaryMPTIndexProof为空\n")
	} else {
		sedbProof.secondaryMPTIndexProof.PrintMPTProof()
	}
	//打印secondaryMBTIndexProof
	if sedbProof.secondaryMBTIndexProof == nil {
		fmt.Printf("secondaryMBTIndexProof为空\n")
	} else {
		sedbProof.secondaryMBTIndexProof.PrintMBTProof()
	}
	//打印secondaryMEHTIndexProof
	if sedbProof.secondaryMEHTIndexProof == nil {
		fmt.Printf("secondaryMEHTIndexProof为空\n")
	} else {
		sedbProof.secondaryMEHTIndexProof.PrintMEHTProof()
	}
}
