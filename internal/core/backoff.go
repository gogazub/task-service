package core

import "time"

// ExpJitter вычисляет экспоненциальный бэкофф с полным джиттером.
// attempt >= 1, rnd() ∈ [0,1).
func ExpJitter(attempt int, base, max time.Duration, rnd func() float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := base << (attempt - 1)
	if d > max {
		d = max
	}
	return time.Duration(float64(d) * rnd())
}
