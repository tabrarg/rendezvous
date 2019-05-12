#ifndef CALLBACK_H
#define CALLBACK_H

#include "tibrv.h"

typedef void (*callback_fn) ();

void c_to_go_callback(tibrvEvent event, tibrvMsg message, void* closure);

#endif