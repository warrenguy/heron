#include "textflag.h"

// DMB ISHLD orders earlier loads before later loads and stores.
TEXT atomicbuffer_acquireFence(SB), NOSPLIT|NOFRAME, $0-0
	DMB $0x9
	RET
