package manager

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.uber.org/zap"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	TokenAudience = "db.mrhachi.dev:mongodb-manager"
	apiGroup      = "db.mrhachi.dev"

	localhostUri = "mongodb://localhost:27017"
	// https://www.mongodb.com/docs/manual/reference/error-codes
	unauthorizedCode      = 13
	notYetInitializedCode = 94
)

type MongoManager struct {
	kube                             *kubernetes.Clientset
	namespace, hostname, serviceName string
	rsName                           string

	logger *zap.Logger
	mu     sync.Mutex
	client *mongo.Client
}

func NewMongoManager(ctx context.Context, logger *zap.Logger) (*MongoManager, error) {
	// Read hostname via syscall
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("get hostname: %w", err)
	}
	namespace := os.Getenv("NAMESPACE")
	if namespace == "" {
		return nil, errors.New("NAMESPACE is missing")
	}
	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		return nil, errors.New("SERVICE_NAME is missing")
	}
	rsName := os.Getenv("RS_NAME")
	if serviceName == "" {
		return nil, errors.New("RS_NAME is missing")
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("get in-cluster config: %w", err)
	}

	kube, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("get kubernetes clientset: %w", err)
	}

	client, err := mongo.Connect(
		options.Client().
			ApplyURI("mongodb://localhost:27017").
			SetDirect(true), // Crucial: we need to talk directly to local instance in RSGhost (freshly created) state
	)
	if err != nil {
		return nil, fmt.Errorf("create mongo manager: %w", err)
	}

	return &MongoManager{
		kube:      kube,
		namespace: namespace, hostname: hostname, serviceName: serviceName,
		rsName: rsName,
		logger: logger,
		client: client,
	}, nil
}

// Authenticate with the given credentials against the admin database
func (m *MongoManager) dbAuth(ctx context.Context, username, password string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	client, err := mongo.Connect(
		options.Client().
			ApplyURI(localhostUri).
			SetAuth(options.Credential{
				Username:   username,
				Password:   password,
				AuthSource: "admin",
			}),
	)
	if err != nil {
		return fmt.Errorf("connect with credentials: %w", err)
	}

	if err := client.Database("admin").
		RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).
		Err(); err != nil {
		defer client.Disconnect(ctx)
		return fmt.Errorf("authentication failed: %w", err)
	}

	old := m.client
	m.client = client

	if err := old.Disconnect(ctx); err != nil {
		return fmt.Errorf("disconnect old client: %w", err)
	}

	return nil
}

func (m *MongoManager) authorize(
	verb, resource string,
	next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		auth := r.Header.Get("Authorization")

		if !strings.HasPrefix(auth, "Bearer ") {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		token := strings.TrimPrefix(auth, "Bearer ")

		if token == "" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		// Perform TokenReview and check valid token
		review, err := m.kube.AuthenticationV1().
			TokenReviews().
			Create(ctx,
				&authenticationv1.TokenReview{
					Spec: authenticationv1.TokenReviewSpec{
						Token: token,
						Audiences: []string{
							TokenAudience,
						},
					},
				},
				metav1.CreateOptions{},
			)
		if err != nil {
			m.logger.Error("token review failed", zap.Error(err))
			http.Error(w, "invalid token", http.StatusBadRequest)
			return
		}

		if !review.Status.Authenticated {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		user := review.Status.User

		// Perform SubjectAccessReview and check permissions
		sar, err := m.kube.AuthorizationV1().
			SubjectAccessReviews().
			Create(ctx,
				&authorizationv1.SubjectAccessReview{
					Spec: authorizationv1.SubjectAccessReviewSpec{
						User:   user.Username,
						Groups: user.Groups,
						ResourceAttributes: &authorizationv1.ResourceAttributes{
							Verb:      verb,
							Resource:  resource,
							Namespace: m.namespace,
							Group:     apiGroup,
						},
					},
				},
				metav1.CreateOptions{},
			)
		if err != nil {
			m.logger.Error("subject access review failed", zap.Error(err))
			http.Error(w, "authorization failed", http.StatusInternalServerError)
			return
		}

		if !sar.Status.Allowed {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	})
}
