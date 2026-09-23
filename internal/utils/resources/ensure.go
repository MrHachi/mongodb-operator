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

		if err := controllerutil.SetControllerReference(
			owner,
			desired,
			scheme,
		); err != nil {
			return actual, fmt.Errorf("set owner reference: %w", err)
		}

		if err := client.Create(ctx, desired); err != nil {
			return actual, fmt.Errorf("create %T: %w", desired, err)
		}

		return desired, nil
	}

	return actual, nil
}
