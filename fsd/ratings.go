package fsd

type NetworkRating int

const (
	RatingInactive      NetworkRating = -1
	RatingSuspended     NetworkRating = 0
	RatingObserver      NetworkRating = 1
	RatingStudent1      NetworkRating = 2
	RatingStudent2      NetworkRating = 3
	RatingStudent3      NetworkRating = 4
	RatingController1   NetworkRating = 5
	RatingController2   NetworkRating = 6
	RatingController3   NetworkRating = 7
	RatingInstructor1   NetworkRating = 8
	RatingInstructor2   NetworkRating = 9
	RatingInstructor3   NetworkRating = 10
	RatingSupervisor    NetworkRating = 11
	RatingAdministrator NetworkRating = 12
)

// IsSupervisor reports whether the rating may kick clients, broadcast to the
// whole server and interrogate other clients with $CQ INF.
func (r NetworkRating) IsSupervisor() bool { return r >= RatingSupervisor }

const (
	FacilityObserver = 0
	FacilityFSS      = 1
	FacilityDelivery = 2
	FacilityGround   = 3
	FacilityTower    = 4
	FacilityApproach = 5
	FacilityCentre   = 6
	FacilityATIS     = 7
)

var minRatingForFacility = map[int]NetworkRating{
	FacilityFSS:      RatingController1,
	FacilityDelivery: RatingStudent1,
	FacilityGround:   RatingStudent1,
	FacilityTower:    RatingStudent2,
	FacilityApproach: RatingStudent3,
	FacilityCentre:   RatingController1,
	FacilityATIS:     RatingStudent1,
}

// IsAllowedFacilityType reports whether rating alone qualifies for a facility.
// Observer is open to everyone; an unknown facility type is refused.
func IsAllowedFacilityType(rating NetworkRating, facility int) bool {
	if facility == FacilityObserver {
		return true
	}

	minRating, ok := minRatingForFacility[facility]
	if !ok {
		return false
	}

	return rating >= minRating
}

func RatingName(rating NetworkRating) string {
	switch rating {
	case RatingInactive:
		return "INAC"
	case RatingSuspended:
		return "SUS"
	case RatingObserver:
		return "OBS"
	case RatingStudent1:
		return "S1"
	case RatingStudent2:
		return "S2"
	case RatingStudent3:
		return "S3"
	case RatingController1:
		return "C1"
	case RatingController2:
		return "C2"
	case RatingController3:
		return "C3"
	case RatingInstructor1:
		return "I1"
	case RatingInstructor2:
		return "I2"
	case RatingInstructor3:
		return "I3"
	case RatingSupervisor:
		return "SUP"
	case RatingAdministrator:
		return "ADM"
	default:
		return "Unknown"
	}
}

func FacilityName(facility int) string {
	switch facility {
	case FacilityObserver:
		return "OBS"
	case FacilityFSS:
		return "FSS"
	case FacilityDelivery:
		return "DEL"
	case FacilityGround:
		return "GND"
	case FacilityTower:
		return "TWR"
	case FacilityApproach:
		return "APP"
	case FacilityCentre:
		return "CTR"
	case FacilityATIS:
		return "ATIS"
	default:
		return "Unknown"
	}
}
