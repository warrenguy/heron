//go:build !race

#include "textflag.h"

// A plain MOVL is a release store under the x86 total store order.
TEXT atomicbuffer_storeRelease32(SB), NOSPLIT|NOFRAME, $0-12
	MOVQ ptr+0(FP), AX
	MOVL value+8(FP), BX
	MOVL BX, (AX)
	RET

// A plain MOVQ is a release store under the x86 total store order.
TEXT atomicbuffer_storeRelease64(SB), NOSPLIT|NOFRAME, $0-16
	MOVQ ptr+0(FP), AX
	MOVQ value+8(FP), BX
	MOVQ BX, (AX)
	RET
