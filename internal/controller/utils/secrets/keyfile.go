package secrets

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

func GenerateKeyfileData() (string, error) {
	b := make([]byte, 756)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate random bytes (is this a legacy Linux system?): %w", err)
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func FillKeyfile(secret *corev1.Secret) error {
	kf, err := GenerateKeyfileData()
	if err != nil {
		return fmt.Errorf("generate keyfile data: %w", err)
	}
	byteData := map[string][]byte{
		"keyfile": []byte(kf),
	}
	secret.Data = byteData
	return nil
}
