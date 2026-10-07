package controller

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ResourceAction string

const (
	ResourceNoop    ResourceAction = "No-op"
	ResourceCreate  ResourceAction = "Create"
	ResourcePatch   ResourceAction = "Patch"
	ResourceBlocked ResourceAction = "Blocked"
)

// resourceProjector applies desired operator-owned fields to target,
// preserving unowned fields, and reports resource-specific conditions.
// target is a copy of the observed object. desired must not be modified.
// An error blocks applying the proposed changes.
type resourceProjector[T client.Object] func(target, desired T) ([]metav1.Condition, error)

// resourceAssessor preserves fields not owned by this controller and never mutates the observation.
type resourceAssessor[T client.Object] func(actual, desired T) (ResourceAssessment[T], error)

// ResourceAssessment describes an observation and a proposed reconciliation.
type ResourceAssessment[T client.Object] struct {
	Actual, Target T
	Action         ResourceAction
	Conditions     []metav1.Condition
}

// ensure observes a resource and executes its assessment.
func ensure[T client.Object](
	ctx context.Context,
	c client.Client,
	desired T,
	assess resourceAssessor[T],
) error {
	key := client.ObjectKeyFromObject(desired)
	existing := desired.DeepCopyObject().(T)
	assessment := ResourceAssessment[T]{Target: desired, Action: ResourceCreate}
	if err := c.Get(ctx, key, existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get %T %s: %w", desired, key, err)
		}
	} else {
		assessment, err = assess(existing, desired)
		if err != nil {
			return fmt.Errorf("assess %T %s: %w", existing, key, err)
		}
	}

	switch assessment.Action {
	case ResourceNoop:
		return nil
	case ResourceCreate:
		if err := c.Create(ctx, assessment.Target); err != nil {
			return fmt.Errorf("create %T %s: %w", desired, key, err)
		}
	case ResourcePatch:
		if err := c.Patch(ctx, assessment.Target, client.MergeFrom(assessment.Actual)); err != nil {
			return fmt.Errorf("patch %T %s: %w", existing, key, err)
		}
	case ResourceBlocked:
		return fmt.Errorf("reconcile %T %s: resource assessment blocked reconciliation", existing, key)
	default:
		return fmt.Errorf("reconcile %T %s: unknown resource action %q", existing, key, assessment.Action)
	}
	return nil
}

// assessOwnedFields runs project against a copy of the observed object,
// then compares the copy with the observation to determine the action.
// ensure handles missing resources before calling this adapter.
func assessOwnedFields[T client.Object](project resourceProjector[T]) func(T, T) (ResourceAssessment[T], error) {
	return func(actual, desired T) (ResourceAssessment[T], error) {
		assessment := ResourceAssessment[T]{
			Actual: actual,
			Target: actual.DeepCopyObject().(T),
			Action: ResourceNoop,
		}

		var err error
		assessment.Conditions, err = project(assessment.Target, desired)
		if err != nil {
			assessment.Action = ResourceBlocked
			return assessment, fmt.Errorf("assess owned fields of %T: %w", actual, err)
		}

		if !equality.Semantic.DeepEqual(actual, assessment.Target) {
			assessment.Action = ResourcePatch
		}
		return assessment, nil
	}
}
