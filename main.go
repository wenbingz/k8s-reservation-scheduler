package main

import (
	"github.com/wenbingz/k8s-reservation-scheduler/pkg/scheduler"
	"k8s.io/apimachinery/pkg/util/wait"
	"time"
)

func main() {
	reservationScheduler := scheduler.NewReservationScheduler()
	wait.Until(reservationScheduler.RunController, time.Second, reservationScheduler.Stopper)
}
