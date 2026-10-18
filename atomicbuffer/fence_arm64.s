#include "textflag.h"

// DMB ISHLD orders earlier loads before later loads and stores.
TEXT atomicbuffer_acquireFence(SB), NOSPLIT|NOFRAME, $0-0
	DMB $0x9
	RET

// DMB ISH orders earlier loads and stores before later loads and stores.
TEXT atomicbuffer_releaseFence(SB), NOSPLIT|NOFRAME, $0-0
	DMB $0xb
	RET
