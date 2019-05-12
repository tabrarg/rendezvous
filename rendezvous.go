package rendezvous

/*
#cgo !windows CFLAGS: -I/path/to/tibrv/include
#cgo !windows LDFLAGS: -L/path/to/tibrv/lib -ltibrv64
#cgo windows CFLAGS: -I/path/to/tibrv/include -I/path/to/mingw32/include --sysroot /path/to/tibrv/include
#cgo windows LDFLAGS: -L/path/to/tibrv/lib -ltibrv -L/path/to/mingw32/lib --sysroot /path/to/tibrv/include
#include "tibrv.h"
#include "callback.h"
#include <stdlib.h>
*/
import "C"
import (
	"log"
	"time"
	"unsafe"
)

type RVParams struct {
	Service  string
	Network  string
	Daemon   string
	Subjects []string
}

type RVMessage struct {
	Timestamp    time.Time
	SendSubject  string
	ReplySubject string
	Message      string
}

var RVMessages chan RVMessage

// Helper function for creating C arrays of strings
func goSliceToCArray(s []string) **C.char {
	cArray := C.malloc(C.size_t(len(s)) * C.size_t(unsafe.Sizeof(uintptr(0))))

	// Convert the C array to a Go Array so we can index it
	a := (*[1<<30 - 1]*C.char)(cArray)

	for idx, substring := range s {
		a[idx] = C.Cstring(substring)
	}

	return (**C.char)(cArray)
}

// Callback function
func GoCallback(cEvent C.tibrvEvent, cMessage C.tibrvMsg, closure *unsafe.Pointer) {
	var sendSubject, replySubject, message *C.char

	C.tibrvMsg_GetSendSubject(cMessage, &sendSubject)
	C.tibrvMsg_GetReplySubject(cMessage, &replySubject)
	C.tibrvMsg_ConvertToString(cMessage, &message)

	RVMessages <- RVMessage{time.Now(), C.GoString(sendSubject), C.GoString(replySubject), C.GoString(message)}
}

// Initialise RV
// Example RV paramaters: []string{"-reliability", "3"}
func InitRV(rvParams []string) {
	log.Println("Info: Creating the internal RV machinery IPM")
	// Create the IPM
	b := C.tibrc_IsIPM()
	if b != 0 {
		log.Fatal("Fatal: Failed to initialise the tibrv_IsIPM function")
	}

	// Set RV parameters
	log.Println("Info: Setting RV parameters:", rvParams)
	err := C.tibrv_SetRVParameters(C.tibrv_u32(len(rvParams)), goSliceToCArray(rvParams))
	if err != C.TIBRV_OK {
		log.Println("Warning: Failed to set RV paramaters for the IMP:", C.GoString(C.tibrvStatus_GetText(err)))
	}

	// Start RV
	log.Println("Info: Opening RV")
	err = C.tibrv_Open()
	if err != C.TIBRV_OK {
		log.Fatal("Fatal: Failed to open RV:", C.GoString(C.tibrvStatus_GetText(err)))
	}
}

// Create a transport and start listening for messages
func StartRV(params RVParams) *chan RVMessage {
	RVMessages = make(chan RVMessage)

	// Create the transport
	var transport C.tibrvTransport

	err := C.tibrvTransport_Create(&transport, C.CString(params.Service), C.CString(params.Network), C.CString(params.Daemon))
	if err != C.TIBRV_OK {
		log.Fatal("Fatal: Failed to initialize transport for subjects:", params.Subjects, ":", C.GoString(C.tibrvStatus_GetText(err)))
	}

	// Start listening to subjects
	go func() {
		var listenID C.tibrvEvent

		for _, subject := range params.Subjects {
			log.Println("Info: Listening to subject:", subject)
			err = C.tibrvEvent_CreateListener(&listenID, C.TIBRV_DEFAULT_QUEUE, C.callback_fn(C.c_to_go_callback), transport, C.CString(subject), nil)
			if err != C.TIBRV_OK {
				log.Fatal("Fatal: Failed to listen to subject:", C.GoString(C.tibrvStatus_GetText(err)))
			}

			// Read events of the queue
			for {
				err = C.tibrvQueue_TimedDispatch(C.TIBRV_DEFAULT_QUEUE, C.TIBRV_WAIT_FOREVER)
				if err != C.TIBRV_OK {
					log.Fatal("Fatal: Failed while reading messages:", C.GoString(C.tibrvStatus_GetText(err)))
				}
			}
		}
	}()

	return &RVMessages
}
