package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// SecretReconciler watches Secrets. For now it only logs what it sees.
type SecretReconciler struct {
	client.Client
}

// Reconcile is called whenever a Secret is created, updated or deleted.
func (r *SecretReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var secret corev1.Secret
	if err := r.Get(ctx, req.NamespacedName, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("secret not found (deleted or unlabeled)")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.Info("reconciling secret",
		"type", secret.Type,
		"keys", len(secret.Data),
		"resourceVersion", secret.ResourceVersion,
	)
	return ctrl.Result{}, nil
}

// SetupWithManager registers this reconciler with the manager.
func (r *SecretReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Secret{}).
		Named("secret").
		Complete(r)
}
