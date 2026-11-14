package auth

import (
	"context"
	"fmt"

	authnv1 "k8s.io/api/authentication/v1"
	authzv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/MrHachi/mongodb-operator/internal/manager/pkg/config"
)

const (
	mongodbResource = "mongodbs"
	apiGroup        = "db.mrhachi.dev"
)

type Authenticator struct {
	clientset *kubernetes.Clientset
}

func NewAuthenticator(config *rest.Config) (*Authenticator, error) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes clientset: %w", err)
	}
	return &Authenticator{clientset: clientset}, nil
}

// VerifyOperatorToken checks if the token is valid and has the necessary permissions.
func (a *Authenticator) VerifyOperatorToken(ctx context.Context, token, namespace, verb string) error {
	review, err := a.reviewOperatorToken(ctx, token)
	if err != nil {
		return err
	}

	// Check permission to perform the requested verb on the MongoDB resource.
	sar := &authzv1.SubjectAccessReview{
		Spec: authzv1.SubjectAccessReviewSpec{
			User:   review.Status.User.Username,
			Groups: review.Status.User.Groups,
			ResourceAttributes: &authzv1.ResourceAttributes{
				Verb:      verb,
				Resource:  mongodbResource,
				Namespace: namespace,
				Group:     apiGroup,
			},
		},
	}

	sarResult, err := a.clientset.AuthorizationV1().SubjectAccessReviews().Create(ctx, sar, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create subject access review: %w", err)
	}

	if !sarResult.Status.Allowed {
		return fmt.Errorf("user %s is not allowed to %s %s in namespace %s: %v", review.Status.User.Username, verb, mongodbResource, namespace, sarResult.Status.Reason)
	}

	return nil
}

// VerifyOperatorServiceAccountToken accepts only a valid instance-manager token
// issued to the configured operator ServiceAccount.
func (a *Authenticator) VerifyOperatorTokenIdentity(ctx context.Context, token, namespace, name string) error {
	if namespace == "" || name == "" {
		return fmt.Errorf("operator ServiceAccount identity is not configured")
	}
	review, err := a.reviewOperatorToken(ctx, token)
	if err != nil {
		return err
	}

	expectedUsername := fmt.Sprintf("system:serviceaccount:%s:%s", namespace, name)
	if review.Status.User.Username != expectedUsername {
		return fmt.Errorf("token belongs to ServiceAccount %q, expected operator ServiceAccount %q", review.Status.User.Username, expectedUsername)
	}
	return nil
}

func (a *Authenticator) reviewOperatorToken(ctx context.Context, token string) (*authnv1.TokenReview, error) {
	tokenReview := &authnv1.TokenReview{
		Spec: authnv1.TokenReviewSpec{
			Token:     token,
			Audiences: []string{config.InstanceManagerAudience},
		},
	}

	review, err := a.clientset.AuthenticationV1().TokenReviews().Create(ctx, tokenReview, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create token review: %w", err)
	}

	if !review.Status.Authenticated {
		return nil, fmt.Errorf("token is not authenticated: %s", review.Status.Error)
	}
	return review, nil
}
