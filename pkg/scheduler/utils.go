package scheduler

import "github.com/wenbingz/k8s-resource-reservation/api/v1alpha1"

func GetReservationKey(reservation *v1alpha1.Reservation) string {
	return reservation.Namespace + "/" + reservation.Name
}
