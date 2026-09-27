package resources

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Ensure fetches the resource identified by desired from the Kubernetes API server.
// If the resource exists, it populates actual with the existing resource and returns it.
// If the resource does not exist (IsNotFound), it applies the optional modifier functions
// to desired, sets owner as the controller owner reference (if owner is non-nil), creates
// the desired resource on the API server, and returns desired.
// The owner parameter is optional-pass nil for cluster-scoped resources or un-owned objects
// to avoid scope mismatched owner errors.
func Ensure[T client.Object](
	ctx context.Context,
	owner client.Object,
	client client.Client,
	scheme *runtime.Scheme,
	desired T,
	actual T,
	mods ...func(T) error,
) (T, error) {
	key := types.NamespacedName{
		Namespace: desired.GetNamespace(),
		Name:      desired.GetName(),
	}

	if err := client.Get(ctx, key, actual); err != nil {
		if !apierrors.IsNotFound(err) {
			return actual, fmt.Errorf("get %T: %w", desired, err)
		}

		for idx, mod := range mods {
			if err := mod(desired); err != nil {
				return actual, fmt.Errorf("apply modifier %d: %w", idx, err)
			}
		}

		if owner != nil {
			if err := controllerutil.SetControllerReference(
				owner,
				desired,
				scheme,
			); err != nil {
				return actual, fmt.Errorf("set owner reference: %w", err)
			}
		}

		if err := client.Create(ctx, desired); err != nil {
			return actual, fmt.Errorf("create %T: %w", desired, err)
		}

		return desired, nil
	}

	return actual, nil
}
