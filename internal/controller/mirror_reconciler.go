package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// Adapter holds the kind-specific parts of mirroring. Everything else
// (targets, ownership, create/update, watching copies) is shared.
type Adapter[T client.Object] interface {
	// New returns an empty object of the mirrored kind.
	New() T
	// CopyData copies the kind-specific payload of src into dst.
	CopyData(dst, src T)
	// CanUpdate reports whether dst can be updated in place to match src.
	// If not, reason explains why (e.g. an immutable field changed).
	CanUpdate(dst, src T) (ok bool, reason string)
}

// MirrorReconciler copies labeled objects of kind T into the namespaces listed
// in their kube-mirror.io/target-namespaces annotation.
type MirrorReconciler[T client.Object] struct {
	client.Client

	// Mirrors is the cache of copies created by kube-mirror (managed-by=kube-mirror).
	// We read copies from it and watch it so changes to copies are noticed.
	// The embedded Client only sees sources (kube-mirror.io/enabled=true).
	Mirrors cache.Cache

	// Name is the controller name; it must be unique per manager.
	Name string

	// Adapter provides the kind-specific behavior.
	Adapter Adapter[T]
}

// Reconcile is called when a source changes, or when one of its copies changes.
func (r *MirrorReconciler[T]) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	src := r.Adapter.New()
	if err := r.Get(ctx, req.NamespacedName, src); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("source not found (deleted or unlabeled)")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	targets := targetNamespaces(src)
	logger.Info("reconciling", "targets", targets)

	// Keep going on failure so one bad namespace doesn't block the others.
	var errs []error
	for _, ns := range targets {
		if err := r.syncTo(ctx, src, ns); err != nil {
			errs = append(errs, fmt.Errorf("sync to namespace %q: %w", ns, err))
		}
	}
	// A non-nil error makes controller-runtime retry with backoff.
	return ctrl.Result{}, errors.Join(errs...)
}

// syncTo makes sure namespace ns holds an up-to-date copy of src.
func (r *MirrorReconciler[T]) syncTo(ctx context.Context, src T, ns string) error {
	// Every log line below (including in helpers) carries the target namespace.
	ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("target", ns))

	ready, err := r.namespaceReady(ctx, ns)
	if err != nil || !ready {
		return err
	}

	dst, found, err := r.getCopy(ctx, ns, src.GetName())
	if err != nil {
		return err
	}
	if !found {
		return r.createCopy(ctx, src, ns)
	}
	return r.updateCopy(ctx, dst, src)
}

// namespaceReady reports whether ns exists and is not being deleted.
func (r *MirrorReconciler[T]) namespaceReady(ctx context.Context, ns string) (bool, error) {
	logger := log.FromContext(ctx)

	var namespace corev1.Namespace
	if err := r.Get(ctx, client.ObjectKey{Name: ns}, &namespace); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("target namespace not found, skipping")
			return false, nil
		}
		return false, err
	}
	if namespace.Status.Phase == corev1.NamespaceTerminating {
		logger.Info("target namespace is terminating, skipping")
		return false, nil
	}
	return true, nil
}

// getCopy reads our copy named name in namespace ns from the mirror cache.
func (r *MirrorReconciler[T]) getCopy(ctx context.Context, ns, name string) (T, bool, error) {
	var zero T

	dst := r.Adapter.New()
	err := r.Mirrors.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, dst)
	if apierrors.IsNotFound(err) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, err
	}
	return dst, true, nil
}

// createCopy creates a new copy of src in namespace ns.
func (r *MirrorReconciler[T]) createCopy(ctx context.Context, src T, ns string) error {
	logger := log.FromContext(ctx)

	dst := r.Adapter.New()
	dst.SetNamespace(ns)
	dst.SetName(src.GetName())
	r.applySource(dst, src)

	if err := r.Create(ctx, dst); err != nil {
		// The mirror cache only sees our copies, so AlreadyExists means
		// someone else owns an object with this name. Never overwrite it.
		if apierrors.IsAlreadyExists(err) {
			logger.Info("object exists and is not managed by kube-mirror, skipping")
			return nil
		}
		return err
	}
	logger.Info("copy created")
	return nil
}

// updateCopy brings an existing copy in line with src, CreateOrUpdate style:
// apply the desired state and send an update only if something changed.
func (r *MirrorReconciler[T]) updateCopy(ctx context.Context, dst, src T) error {
	logger := log.FromContext(ctx)

	// Two sources may share a name; only touch copies of this one.
	if owner := dst.GetAnnotations()[AnnotationSource]; owner != sourceKey(src) {
		logger.Info("copy belongs to another source, skipping", "owner", owner)
		return nil
	}
	if ok, reason := r.Adapter.CanUpdate(dst, src); !ok {
		logger.Info("copy cannot be updated in place, skipping", "reason", reason)
		return nil
	}

	before := dst.DeepCopyObject()
	r.applySource(dst, src)
	if equality.Semantic.DeepEqual(before, dst) {
		return nil
	}
	if err := r.Update(ctx, dst); err != nil {
		return err
	}
	logger.Info("copy updated")
	return nil
}

// applySource writes the desired state of a copy of src into dst.
// It only touches the fields kube-mirror owns.
func (r *MirrorReconciler[T]) applySource(dst, src T) {
	labels := dst.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[LabelManagedBy] = ManagedByValue
	dst.SetLabels(labels)

	annotations := dst.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[AnnotationSource] = sourceKey(src)
	dst.SetAnnotations(annotations)

	r.Adapter.CopyData(dst, src)
}

// mapCopyToSource turns an event on a copy (create, update or delete) into a
// reconcile request for its source, so the source's Reconcile repairs the copy.
// On delete events dst is the copy's last known state, so the annotation is still there.
func (r *MirrorReconciler[T]) mapCopyToSource(_ context.Context, dst T) []reconcile.Request {
	key, ok := parseSourceKey(dst.GetAnnotations()[AnnotationSource])
	if !ok {
		return nil
	}
	return []reconcile.Request{{NamespacedName: key}}
}

// SetupWithManager registers this reconciler with the manager.
func (r *MirrorReconciler[T]) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		// Sources: events from the manager's cache (kube-mirror.io/enabled=true).
		For(r.Adapter.New()).
		// Copies: events from the mirror cache, redirected to their source.
		WatchesRawSource(source.Kind(r.Mirrors, r.Adapter.New(),
			handler.TypedEnqueueRequestsFromMapFunc(r.mapCopyToSource))).
		Named(r.Name).
		Complete(r)
}

// sourceKey returns the "namespace/name" value stored in kube-mirror.io/source.
func sourceKey(src client.Object) string {
	return client.ObjectKeyFromObject(src).String()
}

// parseSourceKey is the inverse of sourceKey. Kubernetes names cannot contain
// "/", so splitting on the first one is unambiguous.
func parseSourceKey(v string) (types.NamespacedName, bool) {
	ns, name, ok := strings.Cut(v, "/")
	if !ok || ns == "" || name == "" {
		return types.NamespacedName{}, false
	}
	return types.NamespacedName{Namespace: ns, Name: name}, true
}
