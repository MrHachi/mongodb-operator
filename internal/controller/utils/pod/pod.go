package pod

import (
	"net/netip"

	corev1 "k8s.io/api/core/v1"
)

// GetAllPodIPs returns the list of IPv4 addresses, then the list of IPv6 addresses
// for the given pod.
func GetAllPodIPs(pod *corev1.Pod) ([]string, []string) {
	var ipv4s []string
	var ipv6s []string

	for _, podIP := range pod.Status.PodIPs {
		if podIP.IP == "" {
			continue
		}

		addr, err := netip.ParseAddr(podIP.IP)
		if err != nil {
			continue // Skip malformed IP strings
		}

		if addr.Is4() {
			ipv4s = append(ipv4s, podIP.IP)
		} else if addr.Is6() {
			ipv6s = append(ipv6s, podIP.IP)
		}
	}

	return ipv4s, ipv6s
}

// IsPodRunning checks that the given Pod is in Running status
func IsPodRunning(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodRunning
}

// GetPodIP returns an IP address belonging to the given Pod.
// It prioritizes IPv6 addresses, but will choose IPv4 if none are found.
// This should be called on a recently refreshed Pod object.
func GetPodIP(pod *corev1.Pod) (string, bool) {
	ipv4, ipv6 := GetAllPodIPs(pod)

	if len(ipv6) > 0 {
		return ipv6[0], true
	} else if len(ipv4) > 0 {
		return ipv4[0], true
	} else {
		return "", false
	}
}
