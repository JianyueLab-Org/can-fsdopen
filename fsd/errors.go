package fsd

// FSD server error codes, sent in $ER packets.
//
// The Python server sent almost none of these — a rejected login was closed
// with a plain text message, and most failures were swallowed entirely, so
// clients hung with no indication of what went wrong. Every rejection path
// here names its code.
const (
	ErrCodeNone                     = 0
	ErrCodeCallsignInUse            = 1
	ErrCodeCallsignInvalid          = 2
	ErrCodeAlreadyRegistered        = 3
	ErrCodeSyntax                   = 4
	ErrCodeSourceInvalid            = 5
	ErrCodeInvalidLogon             = 6
	ErrCodeNoSuchCallsign           = 7
	ErrCodeNoFlightPlan             = 8
	ErrCodeNoWeatherProfile         = 9
	ErrCodeInvalidRevision          = 10
	ErrCodeRequestedLevelTooHigh    = 11
	ErrCodeServerFull               = 12
	ErrCodeCertificateSuspended     = 13
	ErrCodeInvalidControl           = 14
	ErrCodeInvalidPositionForRating = 15
	ErrCodeUnauthorizedSoftware     = 16
	ErrCodeAuthTimeout              = 17
	ErrCodeUnknown                  = 18
)
